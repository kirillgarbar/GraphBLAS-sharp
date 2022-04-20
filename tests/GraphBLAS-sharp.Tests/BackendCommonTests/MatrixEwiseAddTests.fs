module Backend.EwiseAdd

open System
open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests.Utils
open OpenCL.Net

let logger = Log.create "EwiseAdd.Tests"

let checkResult isEqual op zero (baseMtx1: 'a [,]) (baseMtx2: 'a [,]) (actual: Matrix<'a>) =
    let rows = Array2D.length1 baseMtx1
    let columns = Array2D.length2 baseMtx1
    Expect.equal columns actual.ColumnCount "The number of columns should be the same."
    Expect.equal rows actual.RowCount "The number of rows should be the same."

    let expected2D = Array2D.create rows columns zero

    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            expected2D.[i, j] <- op baseMtx1.[i, j] baseMtx2.[i, j]

    let actual2D = Array2D.create rows columns zero

    match actual with
    | MatrixCOO actual ->
        for i in 0 .. actual.Rows.Length - 1 do
            actual2D.[actual.Rows.[i], actual.Columns.[i]] <- actual.Values.[i]
    | _ -> failwith "Impossible case."
//    printfn "Actual \n %A" actual2D
//    printfn "Expected \n %A" expected2D
    for i in 0 .. rows - 1 do
        for j in 0 .. columns - 1 do
            Expect.isTrue (isEqual actual2D.[i, j] expected2D.[i, j]) "Values should be the same."

let correctnessGenericTest
    zero
    op
    (addFun: MailboxProcessor<Msg> -> Backend.Matrix<'a> -> Backend.Matrix<'b> -> Backend.Matrix<'c>)
    toCOOFun
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase)
    (leftMatrix: 'a [,], rightMatrix: 'a [,])
    =
    let q = case.ClContext.CommandQueue
    q.Error.Add(fun e -> failwithf "%A" e)
//    let leftMatrix = Array2D.zeroCreate 5 5
//    let rightMatrix = Array2D.zeroCreate 5 5
//    leftMatrix.[0, 1] <- 1
//    leftMatrix.[1, 0] <- 10
//    leftMatrix.[1, 1] <- 100
//    leftMatrix.[1, 2] <- 1000
//    leftMatrix.[2, 0] <- 10000
//    rightMatrix.[0, 0] <- 2
//    rightMatrix.[1, 0] <- 20
//    rightMatrix.[1, 1] <- 200
//    rightMatrix.[1, 3] <- 2000
//    rightMatrix.[2, 2] <- 20000
//    let mtx1 =
//        { RowCount = 5
//          ColumnCount = 5
//          Rows = [|0; 1; 1; 1; 2|]
//          Columns = [|1; 0; 1; 2; 0|]
//          Values = [|1; 10; 100; 1000; 10000|]
//    }
//    let mtx1 = MatrixCOO mtx1
//    let mtx2 =
//        { RowCount = 5
//          ColumnCount = 5
//          Rows = [|0; 1; 1; 1; 2|]
//          Columns = [|0; 0; 1; 3; 2|]
//          Values = [|2; 20; 200; 2000; 20000|]
//    }
//    let mtx2 = MatrixCOO mtx2
    let mtx1 =
        createMatrixFromArray2D case.MatrixCase leftMatrix (isEqual zero)

    let mtx2 =
        createMatrixFromArray2D case.MatrixCase rightMatrix (isEqual zero)

    if mtx1.NNZCount > 0 && mtx2.NNZCount > 0 then
        let m1 = mtx1.ToBackend case.ClContext
        let m2 = mtx2.ToBackend case.ClContext

        let res = addFun q m1 m2

        m1.Dispose()
        m2.Dispose()

        let cooRes = toCOOFun q res
        let actual = Matrix.FromBackend q cooRes

        cooRes.Dispose()
        res.Dispose()

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        checkResult isEqual op zero leftMatrix rightMatrix actual

let testFixtures case =
    [ let config = defaultConfig
      let wgSize = 128
      //Test name on multiple devices can be duplicated due to the ClContext.toString
      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A, %A" datatype case (System.Random().Next())

      let boolSum1 = <@
          fun (x: bool option) (y: bool option) ->
            let mutable res = false
            match x, y with
            | Some f, Some s -> res <- f || s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()
            if res = false then None else (Some res)
      @>

      let boolSum2 = <@
          fun (x: bool option) (y: bool option) ->
            match x, y with
            | Some f, Some s -> Some (f || s)
            | Some f, None -> Some f
            | None, Some s -> Some s
            | None, None -> None
      @>

      let boolAdd =
          Matrix.eWiseAdd case.ClContext boolSum1 wgSize

      let boolToCOO = Matrix.toCOO case.ClContext wgSize

      case
      |> correctnessGenericTest false (||) boolAdd boolToCOO (=)
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")
      ]

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu
            )
    |> List.collect testFixtures
    |> testList "Backend.Matrix.eWiseAdd tests"
