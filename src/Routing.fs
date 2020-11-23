namespace Hobbes.Web 

open FSharp.Control.Tasks.V2.ContextInsensitive
open Saturn
open Giraffe
open Microsoft.AspNetCore.Http
open Hobbes.Web.Security
open System
open Hobbes.Web.Reflection
open FSharp.Quotations
open Hobbes.Web

module Routing =
    
    let private watch = 
        let w = Diagnostics.Stopwatch()
        w.Start()
        w

    let private verify (ctx : HttpContext) =
        let authToken = 
            let url = ctx.GetRequestUrl()
            printfn "Requesting access to %s" url
            match Uri(url) with
            uri when String.IsNullOrWhiteSpace(uri.UserInfo) |> not ->
                uri.UserInfo |> Some
            | _ -> 
                ctx.TryGetRequestHeader "Authorization"
                
        authToken
        |> Option.bind(fun authToken ->
            if authToken |> verifyAuthToken then
                Some authToken
            else 
                None
        ) |> Option.isSome
               
    let rec private execute name f : HttpHandler =
        fun next (ctx : HttpContext) ->
            task {
                let start = watch.ElapsedMilliseconds
                let code, body = f()
                let ``end`` = watch.ElapsedMilliseconds
                Log.timed name (start - ``end``)
                return! (setStatusCode code >=> setBodyFromString body) next ctx
            }

    let noArgs name f = execute name f

    let withArgs name f args =  
        execute name (fun () -> f args)

    let withBody name f args : HttpHandler = 
        fun next (ctx : HttpContext) ->
            task {
                let! body = ctx.ReadBodyFromRequestAsync()
                let f = f body
                return! ((withArgs name (f) args) next ctx)
            } 

    let withBodyNoArgs name f : HttpHandler = 
        let f = (fun body _ -> f body)
        fun next (ctx : HttpContext) ->
            task {
                let! body = ctx.ReadBodyFromRequestAsync()
                let f = f body
                return! ((noArgs name f) next ctx)
            }

    let verifiedPipe = 
        pipeline {
            plug (fun next ctx -> 
                    if verify ctx then
                        (setStatusCode 200) next ctx
                    else
                        (setStatusCode 403 >=> setBodyFromString "unauthorized") next ctx
                )
        }

    type HttpMethods = 
        Get = 1
        | Post = 2
        | Put = 3
        | Delete = 4

    [<AttributeUsage(AttributeTargets.Class, 
                            Inherited = false, 
                            AllowMultiple = false)>]
    type RouteAreaAttribute(path : string, shouldAuthenticate : bool) = 
        inherit Attribute()
        member __.Path with get() = path
        member __.ShouldAuthenticate with get() = shouldAuthenticate
        new(path) = RouteAreaAttribute(path, true)

    [<AbstractClass>] 
    [<AttributeUsage(AttributeTargets.Method, 
                            Inherited = false, 
                            AllowMultiple = false)>]
    type RouteHandlerAttribute internal (path:string, verb : HttpMethods) =
        inherit Attribute()
        member __.Path with get() = path
        member __.Verb with get() = verb
        abstract HasBody : bool with get

    [<AttributeUsage(AttributeTargets.Method, 
                            Inherited = false, 
                            AllowMultiple = false)>]
    type GetAttribute(path) = 
        inherit RouteHandlerAttribute(path, HttpMethods.Get)
        override __.HasBody with get() = false
    [<AttributeUsage(AttributeTargets.Method, 
                            Inherited = false, 
                            AllowMultiple = false)>]
    type PutAttribute(path, hasBody) = 
        inherit RouteHandlerAttribute(path, HttpMethods.Put)
        override __.HasBody with get() = hasBody

    [<AttributeUsage(AttributeTargets.Method, 
                            Inherited = false, 
                            AllowMultiple = false)>]
    type PostAttribute(path, hasBody) = 
        inherit RouteHandlerAttribute(path, HttpMethods.Post)
        override __.HasBody with get() = hasBody

    [<AttributeUsage(AttributeTargets.Method, 
                            Inherited = false, 
                            AllowMultiple = false)>]
    type DeleteAttribute(path) = 
        inherit RouteHandlerAttribute(path, HttpMethods.Delete)
        override __.HasBody with get() = false
    type RouterBuilder with 
        member private __.FindMethodAndPath<'a> (action: Expr<'a -> int * string>) =
            match readQuotation action with
            [method] when (method :? Reflection.MethodInfo) ->
                let method = method :?> Reflection.MethodInfo
                let att = 
                    match method |> tryGetAttribute<RouteHandlerAttribute> with
                    Some att -> att
                    | None -> failwithf "Route handler must include route handler attribute but '%s' didn't" method.Name
                let path = att.Path
                path, method, att.Verb
            | membr -> failwithf "Don't know what to do with %A" membr 

        member private __.SafeCall (method : Reflection.MethodInfo) (args : obj []) = 
            try
                method.Invoke(null, args) :?> (int * string)
            with e ->
                let rec innerMost (e : Exception) = 
                    if e.InnerException 
                       |> isNull 
                       |> not then 
                       innerMost e.InnerException 
                    else 
                        e
                let e = innerMost e
                    
                Log.excf e "Invocation failed: Method name: %s. Parameters: %s " method.Name (System.String.Join(",",method.GetParameters() |> Array.map(fun p -> p.Name)))
                500, "Call resulted in an invocation error"

        member private this.GenerateRouteWithArgs<'a> state (f : 'a -> int * string) path verb = 
            let pathf = PrintfFormat<_,_,_,_,'a>(path)
            match verb with 
            HttpMethods.Get ->
                this.GetF(state, pathf,(f |> withArgs path))
            | HttpMethods.Post ->
                this.PostF(state, pathf,(f |> withArgs path))
            | HttpMethods.Put ->
                this.PutF(state, pathf,(f |> withArgs path))
            | HttpMethods.Delete ->
                this.DeleteF(state, pathf,(f |> withArgs path))
            | _ -> failwithf "Don't know the verb: %A" verb
        
        member this.LocalFetch(state, path, method : Reflection.MethodInfo) = 
            let f = 
               fun next ctx ->
                    let status,body = 
                        this.SafeCall method [||]
                    (setStatusCode status >=> setBodyFromString body) next ctx

            this.Get(state,path,f)

        member private this.LocalWithArg(state, path, verb, method : Reflection.MethodInfo) = 
            let f (arg1 : 'a) = 
                this.SafeCall method [|arg1|]
            this.GenerateRouteWithArgs state f path verb

        member private this.LocalWithArgs<'a, 'b>(state, path, verb, method : System.Reflection.MethodInfo) = 
            let f (arg1 : 'a, arg2 : 'b) =  
                this.SafeCall method [|arg1;arg2|]
            this.GenerateRouteWithArgs<('a * 'b)> state f path verb

        member private this.LocalWithArgs3<'a, 'b, 'c>(state, path, verb, method : System.Reflection.MethodInfo) = 
            let f (arg1 : 'a, arg2 : 'b, arg3 : 'c) = 
                this.SafeCall method [|arg1;arg2;arg3|]
            this.GenerateRouteWithArgs<('a * 'b * 'c)> state f path verb

        member private this.LocalWithArgs5<'a, 'b, 'c, 'd, 'e>(state, path, verb, method : System.Reflection.MethodInfo) = 
            let f (arg1 : 'a, arg2 : 'b, arg3 : 'c, arg4 : 'd, arg5 : 'e) = 
                this.SafeCall method [|arg1;arg2;arg3;arg4;arg5|]
            this.GenerateRouteWithArgs<('a * 'b * 'c * 'd * 'e)> state f path verb        

        member private this.LocalWithBody(state, path, verb, method) = 
            let f body = 
                this.SafeCall method [|body|]
            match verb with
            HttpMethods.Post ->
                this.Post(state, path,(f |> withBodyNoArgs path))
            | HttpMethods.Put -> 
                this.Put(state, path,(f |> withBodyNoArgs path))
            | _ -> failwithf "Body is not allowed for verb : %A" verb

        [<CustomOperation("withBody")>]
        member this.WithBody(state, action : Expr<string -> int * string>) : RouterState =
            let path,method,verb = this.FindMethodAndPath action
            this.LocalWithBody(state,path,verb,method)

        [<CustomOperation("fetch")>]
        member this.Fetch(state, action : Expr<unit -> int * string>) : RouterState =
           let path,method,_ = this.FindMethodAndPath action
           this.LocalFetch(state, path, method)

        [<CustomOperation("withArg")>]
        member this.WithArg(state, action : Expr<'a -> int * string>) : RouterState =
            let path,method,verb = this.FindMethodAndPath action
            this.LocalWithArg(state,path, verb, method)
            
        [<CustomOperation("withArgs")>]
        member this.WithArgs(state, action : Expr<('a * 'b) -> int * string>) : RouterState =
            let path,method,verb = this.FindMethodAndPath action
            this.LocalWithArgs<'a, 'b>(state,path, verb, method)
        
        [<CustomOperation("withArgs3")>]
        member this.WithArgs3(state, action : Expr<('a * 'b * 'c) -> int * string>) : RouterState =
            let path,method,verb = this.FindMethodAndPath action
            this.LocalWithArgs3<'a, 'b, 'c>(state,path, verb, method)

        [<CustomOperation("withArgs5")>]
        member this.WithArgs5(state, action : Expr<('a * 'b * 'c * 'd * 'e) -> int * string>) : RouterState =
            let path,method,verb = this.FindMethodAndPath action
            this.LocalWithArgs5<'a, 'b, 'c, 'd, 'e>(state,path, verb, method)        

        [<CustomOperation "collect">]
        member this.Collect(state, areaPath : string) : RouterState =
            let routes, state = 
                let areas = 
                    getModulesWithAttribute<RouteAreaAttribute> (Reflection.Assembly.GetExecutingAssembly())
                    |> Seq.filter(fun area -> 
                        (area.GetCustomAttributes(typeof<RouteAreaAttribute>, false) 
                        |> Array.head
                        :?> RouteAreaAttribute).Path.ToLower() = areaPath.ToLower()
                    )
                    
                let state = 
                    match
                        areas
                        |> Seq.tryFind(fun a -> 
                            (a.GetCustomAttributes(typeof<RouteAreaAttribute>, false) |> Array.head :?> RouteAreaAttribute).ShouldAuthenticate
                        ) with
                    None -> state
                    | Some _ ->
                        this.PipeThrough(state, verifiedPipe)
                
                if areas |> Seq.isEmpty then failwithf "Found no modules for %s" areaPath
                areas
                |> Seq.collect(fun area ->
                    area |> getMethodsWithAttribute<RouteHandlerAttribute>
                ), state

            routes
            |> Seq.fold(fun state (att, method) ->
                let noOfArgs = 
                    //%% is a single % escaped. All other % are an argument. Split on % thus results
                    //in an array with one more elements than there are %-args
                    att.Path.Replace("%%", "").Split('%', StringSplitOptions.RemoveEmptyEntries).Length - 1
                let path = areaPath + att.Path
                if att.HasBody then
                    this.LocalWithBody(state,path,att.Verb, method)
                else
                    match noOfArgs with
                    0 -> this.LocalFetch(state, path, method)
                    | 1 -> this.LocalWithArg(state, path, att.Verb, method)
                    | 2 -> this.LocalWithArgs(state, path, att.Verb, method)
                    | 3 -> this.LocalWithArgs3(state, path, att.Verb, method)
                    | 5 -> this.LocalWithArgs5(state, path, att.Verb, method)
                    | _ -> failwithf "Don't know how to handle the arguments of %s" att.Path
            ) state
            