namespace Hobbes.Web

open FSharp.Data
open Hobbes.Helpers.Environment

module Http =
    type Response<'T> = 
        Success of 'T
        | Error of int * string
    
    type CollectorService =
        Read
        | Sync
        with member x.ToPath() =
                "data" ::
                match x with
                Read -> ["read"]
                | Sync -> ["sync"]
    type ConfigurationService =
        Configuration of string option
        | Transformation of string option
        | DependingTransformations of string
        | AllConfigurations
        | Collectors
        | Sources of string
        | Source
        | ConfigurationList
        with member x.ToPath() =
               match x with
               Configuration s -> 
                   "configuration" ::
                     match s with
                     None -> []
                     | Some key -> [key]
               | Transformation s -> 
                   "transformation" ::
                     match s with
                     None -> []
                     | Some key -> [key]
               | Source -> ["source"]
               | Collectors -> ["collectors"]
               | AllConfigurations -> ["allConfigurations"]
               | Sources collector ->
                   ["sources";collector]
               | DependingTransformations cacheKey ->
                   ["dependingtransformations"; cacheKey]
               | ConfigurationList -> ["configurationlist"]
    type CalculatorService =
        Calculate of string
        with member x.ToPath() = 
                match x with
                Calculate key -> 
                    [
                        "calculate"
                        key
                    ]
    type UniformDataService = 
        Read of string
        | Update
        | ReadFormatted of string
        | UpdateFormatted
        with member x.ToPath() =
                match x with
                Read key -> 
                    [
                        "data"
                        "read"
                        key
                    ]
                | Update -> 
                    [
                        "data"
                        "update"
                    ]
                | ReadFormatted key -> 
                    [
                        "dataset"
                        "read"
                        key
                    ]
                | UpdateFormatted -> 
                    [
                        "dataset"
                        "update"
                    ]

    type DbService =
       Root
       | Database of string
       with member x.ToPath() = 
              match x with
              Root -> []
              | Database s -> [s]

    type CalvinService =
        Playground
        | Query
        with member x.ToPath() =
              match x with
              Playground -> ["graphql"]
              | Query -> ["graphql"]
           
    type Service = 
         UniformData of UniformDataService
         | Db of DbService
         | Calculator of CalculatorService
         | Configurations of ConfigurationService
         | Calvin of CalvinService
         with 
            member private x.ToParts() = 
                        let getDnsAndPort (serviceName : string) (defaultPort : int) = 
                           let uc = serviceName.ToUpper()
                           env ( uc + "_DNS") (serviceName + "-svc"), 
                           env (uc + "_PORT") (defaultPort |> string) |> int
                        
                        match x with
                        UniformData serv -> 
                            getDnsAndPort "uniformdata" 8085, serv.ToPath()
                        | Calculator serv -> getDnsAndPort "calculator" 8085,serv.ToPath()
                        | Configurations serv -> getDnsAndPort "configurations" 8085,serv.ToPath()
                        | Db serv -> getDnsAndPort "db" 5984,serv.ToPath()
                        | Calvin serv -> getDnsAndPort "calvin" 8085, serv.ToPath()
            member x.ServiceUrl 
                    with get() = 
                        let (serviceName, port), path = x.ToParts()
                        let pathString = System.String.Join("/",path |> List.map System.Web.HttpUtility.UrlEncode) 
                        sprintf "http://%s:%d/%s"  serviceName port pathString

    let readBody (resp : HttpResponse) =
        match resp.Body with
            | Binary b -> 
                let enc = 
                    match resp.Headers |> Map.tryFind "Content-Type" with
                    None -> System.Text.Encoding.Default
                    | Some s ->
                        s.Split "=" 
                        |> Array.last
                        |> System.Text.Encoding.GetEncoding 
                enc.GetString b
            | Text t -> t
            
    let readResponse parser (resp : HttpResponse)  = 
        if resp.StatusCode <> 200 then
            Error(resp.StatusCode,resp |> readBody)
        else
           resp
           |> readBody
           |> parser
           |> Success
    
    let get (service : Service) parser  = 
        let url = service.ServiceUrl
        printfn "Getting %s" url
        try
            Http.Request(url,
                         httpMethod = "GET",
                         silentHttpErrors = true
            ) |> readResponse parser
        with e ->
            Error(500, sprintf "%s%s %s" url e.Message e.StackTrace)

    let delete (service : Service) parser  = 
        let url = service.ServiceUrl
        printfn "Deleting %s" url
        Http.Request(url,
                     httpMethod = "DELETE",
                     silentHttpErrors = true
        ) |> readResponse parser

    let private putOrPost httpMethod (service : Service) (body : string) = 
        let url = service.ServiceUrl
        printfn "%sting binary to %s" httpMethod url
        let encoding = System.Text.Encoding.UTF8
        try
            Http.Request(url,
                         httpMethod = httpMethod,
                         body = (body |> encoding.GetBytes |> BinaryUpload),
                         headers = [HttpRequestHeaders.ContentTypeWithEncoding("application/json",encoding)],
                         silentHttpErrors = true
            ) |> readResponse id
        with e ->
           eprintf "Error: %s \n Stack: %s" e.Message e.StackTrace
           Error(0, url)

    let put = putOrPost "PUT"
    let post service (body : string) = 
        putOrPost "POST" service body