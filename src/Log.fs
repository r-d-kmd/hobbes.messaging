namespace Hobbes.Web

open FSharp.Data
open Hobbes.Helpers

module Log =
    open FSharp.Core.Printf
    type LogRecord = JsonProvider<"""[{"_id" : "jlk",
                                         "timestamp" : "timestampId",
                                         "type" : "info|debug|error",
                                         "message" : "This is a message",
                                         "stacktrace" : "This is a stacktrace"}, 
                                      {"_id" : "jlk",
                                         "timestamp" : "timestampId",
                                         "type" : "info|debug|error",
                                         "message" : "This is a message"}]""", SampleIsList=true>
    type LogType = 
        Info
        | Debug
        | Error
        | Unknown
        with override x.ToString() = 
                match x with
                | Debug   -> "debug"
                | Info    -> "info"
                | Error   -> "error"
                | Unknown -> "unknown"
             static member Parse (s:string) =
                match s.ToLower() with
                "info"      -> Info
                | "debug"   -> Debug
                | "error"   -> Error
                | "unknown" -> Unknown
                | _         -> 
                    eprintfn "Unknown sync state: %s" s
                    Unknown


    let mutable logLevel = 
        env "LOG_LEVEL" "info" |> LogType.Parse
    
    let mutable private _logger = eprintf "%A" 
    let mutable private _list : unit -> seq<string> = fun () -> Seq.empty
    
    let jsonify (str : string) =
        match str with
        null -> null
        | _ ->
            str.Replace("\"","'")
               .Replace("\\","\\\\")
               .Replace("\n","\\n")
               .Replace("\r","\\r")
               .Replace("\t","\\t")

    let private writeLogMessage (logType : LogType) stacktrace (msg : string) =
        
        if env "LOG_LOCATION" "console" = "console" then
            let maxMessageLength = env "MAX_LOG_LENGTH" "500" |> int
            let esc = string (char 0x1B)
            let red = esc + "[31;1m"
            let noColor = esc + "[0m"
            let yellow = esc + "[1;33m"
            //let's print log messages to console when running locally
            let now() =  System.DateTime.Now.ToString()
            let printer = 
                if logType < logLevel then
                    ignore
                else
                    let p =
                        match logType with
                        Error -> eprintfn "%s%s - %s %s" red
                        | Debug -> printfn "%s%s - %s %s" yellow
                        | _ -> printfn "%s%s - %s %s" noColor
                    fun s -> p (now()) s noColor
            let msg = msg.Substring(0,min msg.Length maxMessageLength)
            (if System.String.IsNullOrWhiteSpace(stacktrace) then
              sprintf "%s" msg
             else
              sprintf "%s StackTrace: \n %s" msg stacktrace)
            |> printer
        else
            async {
                let doc = sprintf """{"timestamp" : "%s",
                             "type" : "%A",
                             "stacktrace" : "%s",
                             "message" : "%s"}""" (System.DateTime.Now.ToString(System.Globalization.CultureInfo.InvariantCulture)) logType (stacktrace |> jsonify) (msg |> jsonify)
               try
                  if logType >= logLevel then
                      doc |>_logger
               with e ->
                   eprintfn "Failed to insert log doc %s. Message: %s StackTrace %s" doc e.Message e.StackTrace
            } |> Async.Start
    let log msg =
        writeLogMessage Info null msg
 
    let error msg = 
        writeLogMessage Error (System.Diagnostics.StackTrace().ToString()) msg

    let debug msg  =
        writeLogMessage Debug null msg
 
    let logf format =
       ksprintf ( writeLogMessage Info null) format
       
    let errorf format = 
       ksprintf (writeLogMessage Error (System.Diagnostics.StackTrace().ToString())) format
    
    let excf (e:System.Exception) format = 
       ksprintf (fun msg -> msg + " Message: " + e.Message |> writeLogMessage Error e.StackTrace) format
    let exc (e:System.Exception) message = excf e "%s" message

    let debugf format =
       ksprintf ( writeLogMessage Debug null) format

    let timed requestName (ms : int64) = 
        let doc = sprintf """{"timestamp" : "%s",
                             "type" : "requestTiming",
                             "requestName" : "%s",
                             "executionTime" : %d}""" (System.DateTime.Now.ToString(System.Globalization.CultureInfo.InvariantCulture)) (requestName |> jsonify) ms
        async {
            try
                doc |> _logger
            with e ->
                eprintfn "Failed to insert timed event in log. %s. Message: %s StackTrace %s" doc e.Message e.StackTrace
        } |> Async.Start

    //_list is mutable. We'll hide that and make sure that the current is always called
    let list() = 
        _list()

    let loggerInstance = 
        { new ILog with
            member __.Log msg   = log msg
            member __.Error msg = error msg
            member __.Debug msg = debug msg
            member __.Logf<'a> (format : LogFormatter<'a>)  = logf format  
            member __.Errorf<'a> (format : LogFormatter<'a>) = errorf format
            member __.Excf<'a> e (format : LogFormatter<'a>) = excf e format
            member __.Exc e msg = exc e msg
            member __.Debugf<'a>  (format : LogFormatter<'a>) = debugf format
        }

    let ignoreLogging =
        { new ILog with
            member __.Log _   = ()
            member __.Error msg = error msg
            member __.Debug _ = ()
            member __.Logf<'a> (format : LogFormatter<'a>)  = 
                ksprintf ignore format
            member __.Errorf<'a> (format : LogFormatter<'a>) = errorf format
            member __.Excf<'a> (e : System.Exception) (format : LogFormatter<'a>) = excf e format
            member __.Exc e msg = excf e "%s" msg
            member __.Debugf<'a>  (format : LogFormatter<'a>)  = 
                ksprintf ignore format
        }
    do
        let db = Database.Database("log", LogRecord.Parse, ignoreLogging)
        _logger <- db.Post >> ignore
        _list <- (db.List >> Seq.map(fun l -> l.JsonValue.ToString(JsonSaveOptions.DisableFormatting)))