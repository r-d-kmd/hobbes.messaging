namespace Hobbes.Web

open Newtonsoft.Json
open Thoth.Json.Net
open Hobbes.Helpers.Environment

module Cache =
     
    type DataResult = 
        {
            [<JsonProperty("columnNames")>]
            ColumnNames : string []
            [<JsonProperty("rows")>]
            Values : obj [][]
            [<JsonProperty("rowCount")>]
            RowCount : int
        } with static member Decode =
                 let decode = 
                     (Decode.array Decode.int)
                 let boxingInt = 
                     Decode.map box
                        Decode.int
                 let boxingString = 
                     Decode.map(fun (s : string) ->
                         match System.DateTime.TryParse s with
                         true,dt -> box dt
                         | _ -> box s) Decode.string
                 let boxingFloat = 
                     Decode.map box
                        Decode.float
                 let boxingDecimal = 
                     Decode.map box
                        Decode.decimal
                 let boxingBool = 
                     Decode.map box
                        Decode.bool
                 let boxingNull = 
                     Decode.map
                        (fun _ -> null)
                 let objDecoder = 
                     [
                         boxingInt
                         boxingString
                         boxingFloat
                         boxingDecimal
                         boxingBool
                         (fun s o -> 
                            let txt = 
                                if o |> isNull then
                                   null
                                else
                                   (o |> string).ToLower()
                            if txt |> System.String.IsNullOrWhiteSpace || txt.ToLower() = "null" then 
                                Ok(null) 
                            else
                                Error(DecoderError(s,ErrorReason.FailMessage (sprintf "%s isn't null. found at %s" txt s)))
                        )
                     ]
                 
                 Decode.object(fun get ->
                     {
                         ColumnNames = get.Required.Field "columnNames" (Decode.array Decode.string)
                         Values = get.Required.Field "values" (Decode.array (Decode.array (Decode.oneOf objDecoder)))
                         RowCount = get.Required.Field "rowCount" Decode.int
                     }
                 )
               static member OfJson (json:string)=
                 match Decode.fromString DataResult.Decode json with
                 Ok dataResult -> dataResult
                 | Error msg -> failwith msg

               member x.Rows() =
                    x.Values
               member x.JsonValue 
                   with get() = 
                       Encode.object [
                           "columnNames", Encode.array (x.ColumnNames |> Array.map Encode.string)
                           "values", Encode.array (x.Values |> Array.map(fun row ->
                                Encode.array (row |> Array.map(fun cell ->
                                    match cell with
                                    null -> Encode.nil
                                    | :? System.DateTime as d -> 
                                        d.ToString() |> Encode.string
                                    | :? System.DateTimeOffset as d -> 
                                        d.ToLocalTime().DateTime.ToString() |> Encode.string
                                    | :? int as i  -> i |> Encode.int
                                    | :? float as f -> f |> Encode.float
                                    | :? decimal as d -> d |> Encode.decimal
                                    | :? string as s  -> s |> Encode.string
                                    | :? bool as b -> b |> Encode.bool
                                    | _ -> failwithf "Don't know how to encode %A" cell
                                )) 
                            ))
                           "rowCount", Encode.int x.RowCount
                       ]
               override x.ToString() = 
                   x.JsonValue
                   |> Encode.toString 0

    type CacheRecord = 
        {
            [<JsonProperty("_id")>]
            CacheKey : string
            [<JsonProperty("timestamp")>]
            TimeStamp : System.DateTime option
            DependsOn : string list
            [<JsonProperty("data")>]
            Data : DataResult
        }
        with static member OfJson (json:string) =
                let decoder =  
                      Decode.object(fun get ->
                          {
                              CacheKey = get.Required.Field "_id" Decode.string
                              TimeStamp = get.Optional.Field "timestamp" (
                                               Decode.map (fun (s : string) -> 
                                                   match System.DateTime.TryParse s with
                                                   true,dt -> dt
                                                   | _ -> 
                                                       Log.errorf "Couldn't parse (%s) as date" s
                                                       failwithf "not a date %s" s
                                               ) Decode.string)
                              DependsOn = get.Required.Field "dependsOn" (Decode.map (fun a -> a |> List.ofArray) (Decode.array Decode.string))
                              Data = get.Required.Field "data" DataResult.Decode
                          }
                      )
                      
                match Decode.fromString decoder json with
                Ok record -> record
                | Error msg -> failwith msg 
             member x.JsonValue 
                   with get() = 
                       Encode.object [
                           yield "_id", Encode.string x.CacheKey
                           if x.TimeStamp.IsSome then 
                               yield "timestamp", Encode.string (x.TimeStamp.Value.ToString())
                           yield "dependsOn", Encode.list(x.DependsOn |> List.map Encode.string) 
                           yield "data", x.Data.JsonValue
                       ]
             override x.ToString() = 
                x.JsonValue
                |> Encode.toString 0

    type DynamicRecord = FSharp.Data.JsonProvider<"""{
        "_id" : "khjkjhkjh",
        "timestamp" : "13/07/2020 11:55:21",
        "dependsOn" : ["lkjlk","lhkjh"],
        "data" : [{"columnName1":"value","columnName2" : 1},{"columnName1":"value","columnName2" : 1}]
    }""">
    
    type BaseRecord = FSharp.Data.JsonProvider<"""{
        "_id" : "khjkjhkjh",
        "dependsOn" : ["lkjlk","lhkjh"]
    }""">

    let key (source : string) = 
        let whitespaceToRemove = [|' ';'\t';'\n';'\r'|]
        source.Split(whitespaceToRemove,System.StringSplitOptions.RemoveEmptyEntries)
        |> System.String.Concat
        |> Hobbes.Helpers.Environment.hash
    
    let inline createCacheRecord key dependsOn (data : DataResult)  =
        let timeStamp = System.DateTime.Now
        {
            CacheKey =  key
            TimeStamp = Some timeStamp
            DependsOn = dependsOn
            Data = data
        }

    let createDynamicCacheRecord key (dependsOn : string list) dataArray =
        assert(key |> System.String.IsNullOrWhiteSpace |> not)

        let dependsOn = 
            dependsOn
            |> List.map Encode.string
            |> Array.ofList
            |> Encode.array
        let dataJson = 
            Encode.array dataArray
        let timeStamp = System.DateTime.Now
        let res = 
            Encode.object
                [
                    "_id",Encode.string key
                    "timestamp", Encode.string (sprintf "%A" timeStamp)
                    "dependsOn", dependsOn
                    "data", dataJson
                ]
            |> Encode.toString 0
            |> DynamicRecord.Parse
        assert(res.Id = key)
        assert(res.Timestamp = (sprintf "%A" timeStamp))
        res

    let readData (cacheRecordText : string) =
        let cacheRecord = CacheRecord.OfJson cacheRecordText 
        let data = cacheRecord.Data
        let columnNames = data.ColumnNames
        
        data.Rows()
        |> Seq.mapi(fun index row ->
            index,(row
                   |> Seq.zip columnNames)
        )

    type Cache<'recordType,'dataType> (name : string, deserializer : string -> 'recordType,serializer : 'recordType -> string , recordCreator : string -> string list -> 'dataType -> 'recordType) =
        let dbName = name + "cache"
        let db = 
            Database.Database(dbName, deserializer, Log.loggerInstance)
        let list = 
            Database.Database(dbName, BaseRecord.Parse, Log.loggerInstance)
            
        member __.InsertOrUpdate key dependsOn data = 
            let serialized = 
                data
                |> recordCreator key dependsOn
                |> serializer
            assert(try 
                     serialized |> deserializer |> ignore
                     true 
                   with e -> 
                      Log.excf e "Roundtrip assertion failed of %A" data
                      false)
            serialized
            |> db.InsertOrUpdate 
            |> Log.debugf "Inserted data: %s"
        
        member __.Get (key : string) = 
            Log.logf "trying to retrieve cached %s from database" key
            key
            |> db.TryGet 
        member __.Peek (key : string) =
            db.TryGetRev key |> Option.isSome
        member cache.Delete (key : string) = 
            Log.logf "Deleting %s" key
            let sc,body= 
                key
                |> db.Delete
            list.List()
            |> Seq.filter(fun doc ->
                doc.DependsOn |> Array.contains key
            ) |> Seq.iter(fun doc -> cache.Delete doc.Id)
            if sc <> 200 then
                failwithf "Status code %d - %s" sc body
    let cache name = 
        Cache(name,
              CacheRecord.OfJson,
              (fun record -> record.JsonValue |> Encode.toString 0),
              createCacheRecord)
    let dynamicCache name = 
        Cache(
            name,
            DynamicRecord.Parse,
            (fun dynRec -> dynRec.JsonValue.ToString()),
            createDynamicCacheRecord
        )