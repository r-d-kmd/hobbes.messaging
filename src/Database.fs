namespace Hobbes.Web
    open FSharp.Data
    open Hobbes.Helpers 
    type ErrorMessage = JsonProvider<"""{"error":"bad_request","reason":"invalid UTF-8 JSON"}""">
    type Logger = string -> unit
    type LogFormatter<'a> = Printf.StringFormat<'a,unit>
    type ILog =
        abstract Log : string -> unit
        abstract Error : string -> unit
        abstract Debug : string -> unit 
        abstract Logf<'a> : LogFormatter<'a> -> 'a
        abstract Errorf<'a> : LogFormatter<'a> -> 'a
        abstract Excf<'a> : System.Exception -> LogFormatter<'a> -> 'a
        abstract Exc : System.Exception -> string -> unit
        abstract Debugf<'a> : LogFormatter<'a> -> 'a
        
    module Database =
        let ServerUrl = "http://db-svc:5984/"
        open FSharp.Core.Printf
        let now() =  System.DateTime.Now.ToString()
        let printer = fun s -> printfn "%s - %s" (now()) s
        let eprinter = fun s -> eprintfn "%s - %s" (now()) s
        let consoleLogger =
                { new ILog with
                    member __.Log msg   = sprintf "%s" msg |> printer
                    member __.Error msg = 
                        sprintf "%s" msg
                        |> eprinter
                    member __.Debug msg = sprintf "%s" msg |> printer
                    member __.Logf<'a> (format : LogFormatter<'a>) = 
                        ksprintf printer format 
                    member __.Errorf<'a> (format : LogFormatter<'a>) = 
                        ksprintf eprinter format
                    member __.Excf<'a> (e : System.Exception) (format : LogFormatter<'a>) = 
                        ksprintf (fun msg -> msg + "Message: " + e.Message |> eprinter) format
                    member this.Exc (e : System.Exception) msg  = 
                        this.Excf e "%s" msg
                    member __.Debugf<'a> (format : LogFormatter<'a>) = 
                        ksprintf printer format
                }  

        let private user = env "COUCHDB_USER" "admin"
        let private pwd = env "COUCHDB_PASSWORD" "password"

        type CouchDoc = JsonProvider<"""{
            "_id" : "dd",
            "_rev": "jlkjkl"}""">

        type Viewdoc = JsonProvider<"""{
            "_id" : "dd",
            "key": "jens",
            "_rev": "jlkjkl"}""">

        type UserRecord = JsonProvider<"""{
          "_id": "org.couchdb.user:dev",
          "_rev": "1-39b7182af5f4dc7a72d1782d808663b1",
          "name": "dev",
          "type": "user",
          "roles": [],
          "password_scheme": "pbkdf2",
          "iterations": 10,
          "derived_key": "492c5c2855d72ae88a5ff5f70f75ddb0ef63b32f",
          "salt": "a6212b3f03511523954fbc93e7c0907d"
        }""">

        type Rev = JsonProvider<"""{"_rev": "osdhfoi94392h329020"}""">
        type Hash = JsonProvider<"""{"hash": "9AFDC4392329020"}""">

        type DataSet = JsonProvider<"""{
            "some column" : ["rowValue1", "rowValue2"],
            "some column2" : ["rowValue1", "rowValue2"]
        }""">

        type List = JsonProvider<"""[{
            "total_rows": 2,
            "offset": 2,
            "rows": [
                {
                    "id": "id1",
                    "key": "id1",
                    "value": {
                        "rev": "1-a4f8ac9d02e65da8251583a0c893e26b"
                    },
                    "doc": {
                        "_id": "id1",
                        "_rev": "1-a4f8ac9d02e65da8251583a0c893e26b",
                        "lines": [
                            "line1",
                            "line2",
                            "line3"
                        ]
                    }
                }]
            },{
         "id": "07e9a2611a712c808bd422425c9dcda2",
         "key": [
          "Azure DevOps",
          "flowerpot"
         ],
         "value": 90060205,
         "doc": {}}]""", SampleIsList = true>

        type private DatabaseName =
            Configurations
            | Transformations
            | Cache
            | RawData
            | Users

        type HttpMethod = 
            Get
            | Post
            | Put
            | Delete

        type ViewList<'a> = 
            {
                TotalRows : int
                Offset : int
                Rows : 'a []
            }

        let private _awaitDbServer() =    
            async {
                let dbUser = 
                    match env "COUCHDB_USER" null with
                    null -> failwith "DB user not configured"
                    | user -> user

                let dbPwd = 
                    match env "COUCHDB_PASSWORD" null with
                    null -> failwith "DB password not configured"
                    | pwd -> pwd
                let rec inner (tries : int) =
                    let retry() = 
                        async{
                            do! Async.Sleep 5000
                            return! inner (tries + 1)
                        }
                    let check url =
                        async {
                            try
                                let! resp = Http.AsyncRequest(
                                                        url,
                                                        httpMethod = "HEAD", 
                                                        silentHttpErrors = true,
                                                        headers = [HttpRequestHeaders.BasicAuth dbUser dbPwd]
                                                       ) 
                                
                                if resp.StatusCode > 299 then
                                   eprintfn "Error checking [%s]. %d - %s" url resp.StatusCode (resp |> Http.readBody)
                                   return! retry()
                             with
                            :? System.UriFormatException as e ->
                                failwithf "Uri (%s) format exception Message: %s. Trace: %s" url e.Message e.StackTrace
                            | e ->
                                if tries % 1000 = 0 then
                                    eprintfn "DB not reachable. Message: %s. Trace: %s" e.Message e.StackTrace
                                return! retry()
                        }
                    async {

                        printfn "Testing of db server is reachable on [%s]" ServerUrl
                        
                        do! check ServerUrl
                        do! check (ServerUrl + "_users")
                        
                    }
                do! (inner 0)
                return ServerUrl,dbUser,dbPwd
            }

        let awaitDbServer() =
            async {
                let! _ = _awaitDbServer()
                return ()
            } |> Async.RunSynchronously

        let rec initDatabases databaseToBeInitialized =
           
            async {
                let httpMethod = "PUT"
                let! databaseServerUrl,dbUser,dbPwd = _awaitDbServer()
                let request dataBaseName = 
                    let url = databaseServerUrl + "/" + dataBaseName
                    printfn "Creating database. %s on %s" httpMethod url
                    Http.Request(url,
                                 httpMethod = httpMethod,
                                 silentHttpErrors = true,
                                 headers = [HttpRequestHeaders.BasicAuth dbUser dbPwd]
                                ) 
                let failed = 
                   databaseToBeInitialized
                   |> List.filter(fun name ->
                    let resp = request name
                      
                    match resp.StatusCode with
                    200 
                    | 201 ->
                       printfn "Database created"
                       false
                    | 412 -> 
                       printfn "Database already existed"
                       false
                    | 401 -> 
                        failwith "DB user not configured correctly" 
                    | _ ->
                       eprintfn "Database creation failed with %d - %s. Will try again" resp.StatusCode (resp |> Http.readBody)
                       true
                   )
                if failed |> List.isEmpty |> not then
                    do! Async.Sleep 2000
                    initDatabases failed
                else
                    printfn "DB initialized"
            } |> Async.Start
  
        let private getBody (resp : HttpResponse) = 
            match resp.Body with
            Binary _ -> failwithf "Can't use a binary response"
            | Text res -> res

        type View(getter : string list -> ((string * string) list) option -> int * string, name, log : ILog) = 

            let _list (startKey : string option) (endKey : string option) limit (descending : bool option) skip = 
                let args = 
                    [ 
                        match  startKey, endKey  with
                          None,None -> ()
                          | Some key,None | None,Some key -> yield "key", key
                          | Some startKey,Some endKey -> 
                              yield "startkey", startKey
                              yield "endkey", endKey
                        match limit with
                          None -> ()
                          | Some l -> yield "limit", string l
                        if descending.IsSome && descending.Value then yield "descending","true"
                        match skip with
                          None -> ()
                          | Some l -> yield "skip", string l
                    ] |> Some
                let path = 
                    [
                     "_design"
                     "default"
                     "_view"
                     name
                    ]
                log.Debugf "Fetching view %A?%A " path args
                getter path args

            let getListFromResponse (statusCode,body) =
                if statusCode < 300 && statusCode >= 200 then
                    log.Debug "Parsing list result"
                    body |> List.Parse
                else
                    log.Errorf  "Error when fetching list: %s" body
                    failwithf "Error: %s" body
            
            let listResult  (startKey : string option) (endKey : string option) limit (descending : bool option) skip =
                _list startKey endKey limit descending skip
                |> getListFromResponse
            
            let rowCount startKey endKey = 
                (listResult startKey endKey (Some 0) None None).TotalRows
                |> Option.orElse(Some 0)
                |> Option.get


            let list (parser : string -> 'a) (startKey : string option) (endKey : string option) (descending : bool option) = 
                let mutable limit = 128
                let rec fetch i acc = 
                    let statusCode,body = _list startKey endKey (Some limit) descending (i |> Some)
                    if statusCode = 500 && limit > 1 then
                        //this is usually caused by an os process time out, due to too many records being returned
                        //gradually shrink the page size and retry
                        limit <- limit / 2
                        fetch i acc
                    else
                        let result = (statusCode,body) |> getListFromResponse
                        let rowCount =  result.Rows |> Array.length |> max limit
                        let values = 
                            match result.Value with
                            Some v -> (v |> string)::acc
                            | _ -> result.Rows |> Array.fold(fun acc entry -> entry.Value.ToString()::acc) acc
                        match result.TotalRows, result.Offset with
                        Some t, Some o when t <= o + rowCount -> values
                        | _ -> fetch (i + rowCount) values

                fetch 0 [] 
                |> List.filter(fun d -> 
                    (CouchDoc.Parse d).Id <> "default_hash"
                ) |> List.map parser
                
            member __.List<'a>(parser : string -> 'a, ?startKey : string, ?endKey : string, ?descending) =
                list parser startKey endKey descending
            member __.List<'a>(parser : string -> 'a, limit, ?startKey : string, ?endKey : string, ?descending) =
                (listResult startKey endKey (Some limit) descending None).Rows
                |> Array.map(fun entry -> entry.Value.ToString() |> parser)

        and Database<'a> (databaseName, parser : string -> 'a, log : ILog) =
            let mutable _views : Map<string,View> = Map.empty
            
            let request httpMethod isTrial (body : string option) path rev queryString =
                let enc (s : string) = System.Web.HttpUtility.UrlEncode s           
                let root = 
                     System.String.Join("/", [
                                                ServerUrl
                                                databaseName
                                            ])
                let urlWithoutQS = 
                    System.String.Join("/", root::(path
                                                   |> List.map enc)) 
                let url = 
                    urlWithoutQS +
                    match queryString with
                    None -> ""
                    | Some qs -> 
                       "?" + System.String.Join("&",
                                             qs
                                             |> Seq.map(fun (k,v) -> sprintf "%s=%s" k  v)
                       )
                let m,direction =
                      match httpMethod with 
                      Get -> "GET", "from"
                      | Post -> "POST", "to"
                      | Put -> "PUT", "to"
                      | Delete -> "DELETE", "from"          
                    
                log.Debugf "%sting %A %s (%s,%A) %s" m url direction root path databaseName
                let encoding = System.Text.Encoding.UTF8
                let headers =
                    [
                        yield HttpRequestHeaders.BasicAuth user pwd
                        yield HttpRequestHeaders.ContentTypeWithEncoding(HttpContentTypes.Json, encoding)
                        if rev |> Option.isSome then yield HttpRequestHeaders.IfMatch rev.Value
                    ]
                let statusCode,body = 
                    try
                        let resp = 
                            match body with
                            None -> 
                                Http.Request(url,
                                    httpMethod = m, 
                                    silentHttpErrors = true,
                                    headers = headers
                                )
                            | Some body ->
                                Http.Request(url,
                                    httpMethod = m, 
                                    silentHttpErrors = true, 
                                    body = (body |> encoding.GetBytes |> BinaryUpload),
                                    headers = headers
                                )
                        resp.StatusCode, resp |> getBody
                    with e ->
                        500, e.Message
                let failed = statusCode < 200 || statusCode >= 300
                if failed then
                    log.Debugf "Response status code : %d.  Body: %s. Url: %s" 
                        statusCode 
                        (body.Substring(0,min 1000 body.Length)) 
                        url
                 
                if isTrial || not(failed) then
                    statusCode,body
                else
                    failwithf "Server error %d. Reason: %s. Url: %s" statusCode body url

            let requestString httpMethod silentErrors body path rev queryString = 
                request httpMethod silentErrors body path rev queryString |> snd
            let tryRequest m rev body path queryString = request m true body path rev queryString
            let get path = requestString Get false None path None
            let put body path rev = requestString Put false (Some body) path rev None
            let post body path = requestString Post false (Some body) path None 
            let tryGet = tryRequest Get None None 
            let tryPut body rev path = tryRequest Put rev (Some body) path None 
            let tryPost body path = tryRequest Post None (Some body) path None 
            let delete id rev = request Delete false None [id] rev None
            
            let databaseOperation operationName argument =
                let path = 
                   match argument with
                   None -> [operationName]
                   | Some a -> [operationName;a]
                requestString Post false None path None

            let handleResponse (statusCode,body : string) = 
                if  statusCode >= 200  && statusCode <= 299  then
                    body
                elif statusCode = 400 then
                    failwithf "Bad format. Doc: %s" (body.Substring(0, min body.Length 500))
                else
                    failwith body
                
            member this.AddView name =
                _views <- _views.Add(name, View(tryGet,name,log))
                this
            
            member __.ListIds() =
                (get ["_all_docs"] None
                 |> List.Parse).Rows
                 |> Array.map(fun r -> r.Id)
                 |> Seq.ofArray

            member __.List() =
                (get ["_all_docs"] (Some ["include_docs","true"])
                 |> List.Parse).Rows
                 |> Array.map(fun r -> r.Doc.JsonValue.ToString JsonSaveOptions.DisableFormatting |> parser)
                 |> Seq.ofArray

            member __.Get id =
                get [id] None |> parser

            member __.Get path =
                get path None |> parser

            member __.TryGet id = 
                let statusCode,body = tryGet [id] None
                if statusCode >= 200  && statusCode <= 299 then
                    body |> parser |> Some
                else
                    None             

            member __.GetRev id =
                (get id None |> Rev.Parse).Rev      

            member __.TryGetRev id = 
                if System.String.IsNullOrWhiteSpace id then failwith "Can't get revision of empty id"
                let statusCode,body = tryGet [id] None
                if statusCode >=200 && statusCode < 300 then
                    let revision = 
                        (body |> Rev.Parse).Rev
                    (if System.String.IsNullOrWhiteSpace(revision) then 
                        failwithf "Invalid revision. %s" body)
                    revision |> Some
                else
                    None

            member __.GetHash id =
                 (get [sprintf "%s_hash" id] None
                  |> Hash.Parse).Hash   

            member __.TryGetHash id = 
                let statusCode,body = tryGet [sprintf "%s_hash" id] None
                if statusCode >= 200 && statusCode < 300 then
                    (body 
                       |> Hash.Parse).Hash 
                       |> Some  
                else None                        
            member __.Put(path, body, ?rev) = 
                put body path rev
            member __.Put(id, body, ?rev) = 
                put body [id] rev
            member __.TryPut(id, body, ?rev) = 
                tryPut body rev [id]
            member __.Post(body) = 
                tryPost body []
                |> handleResponse
            member __.Post(path, body) = 
                tryPost body [path]
                |> handleResponse
            member __.FilterByKeys keys = 
                let body = 
                   System.String.Join(",", 
                       keys
                       |> Seq.map(fun s -> sprintf "%A" s)
                   )
                   |> sprintf """{"keys" : [%s]}"""
                try
                    (post body ["_all_docs"] (Some ["include_docs","true"])
                     |> List.Parse).Rows
                    |> Array.map(fun entry -> 
                        try
                            entry.Doc.ToString() |> parser
                        with e ->
                            failwithf "Failed loading. Row: %A. Msg: %s" (entry.ToString()) e.Message
                    ) |> Seq.ofArray
                with e ->
                    log.Excf e "Failed getting documents by key. POST Body: %s" (body.Substring(0,min body.Length 500))
                    reraise()
            member __.Views with get() = _views
            member this.InsertOrUpdate (record : #Runtime.BaseTypes.IJsonDocument) =
                record.JsonValue.ToString()
                |> this.InsertOrUpdate

            member this.InsertOrUpdate doc =
                 
                let id = (CouchDoc.Parse doc).Id

                assert(System.String.IsNullOrWhiteSpace((CouchDoc.Parse doc).Rev))
                assert(id |> System.String.IsNullOrWhiteSpace |> not)

                let request() = 
                    let rev = id |> this.TryGetRev
                    request Put true (Some doc) [id] rev None
                    
                let status,body = 
                    let st,b = request()  
                    if st = 409 then //this can happen in the case of a race
                       request() //force the update
                    else
                        st,b  
                if (status >= 200 && status < 399) then
                    body
                else
                    let message = ErrorMessage.Parse body
                    if message.Reason = "invalid UTF-8 JSON" then
                       log.Errorf "invalid json. %s" doc
                    else
                       log.Errorf "Failed to update document. %d - %s" status body
                    sprintf """{"status": %d,"message":"%s"}""" status body

            member __.Compact() = 
                databaseOperation "_compact"  None
            
            member __.CompactDesign() = 
                databaseOperation "_compact" (Some "default")
            
            member __.ViewClenaup() = 
                databaseOperation "_view_cleanup" None

            member this.CompactAndClean() = 
                this.Compact() |> ignore
                this.CompactDesign() |> ignore
                this.ViewClenaup() |> ignore
            
            member __.Delete id =
                let doc = 
                    get [id] None
                    |> CouchDoc.Parse
                delete id (Some doc.Rev)
            
            member __.Init() =
                tryPut "" None [] |> fst

            member __.Exists with get() =
                let status = tryGet [] None |> fst
                status > 199 && status < 300

          
        let users = Database ("_users", UserRecord.Parse, consoleLogger)
        let couch = Database ("", id, consoleLogger)