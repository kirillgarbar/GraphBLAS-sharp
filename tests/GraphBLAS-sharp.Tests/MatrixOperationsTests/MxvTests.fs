module Matrix.Mxv

open Expecto
open FsCheck
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Predefined
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open OpenCL.Net

let logger = Log.create "MxvTests"

type OperationCase =
    {
        ClContext: OpenCLEvaluationContext
        MatrixCase: MatrixFromat
        VectorCase: VectorFormat
        MaskCase: MaskType
    }

let testCases =
    [
        Utils.avaliableContexts "" |> Seq.map box
        Utils.listOfUnionCases<MatrixFromat> |> Seq.map box
        Utils.listOfUnionCases<VectorFormat> |> Seq.map box
        Utils.listOfUnionCases<MaskType> |> Seq.map box
    ]
    |> List.map List.ofSeq
    |> Utils.cartesian
    |> List.map
        (fun list -> {
            ClContext = unbox list.[0]
            MatrixCase = unbox list.[1]
            VectorCase = unbox list.[2]
            MaskCase = unbox list.[3]
        })

let checkCorrectnessGeneric<'a when 'a : struct>
    (semiring: ISemiring<'a>)
    (isEqual: 'a -> 'a -> bool)
    (case: OperationCase)
    (matrix: 'a[,], vector: 'a[], mask: bool[]) =

    let isZero = isEqual semiring.Zero

    let expected =
        let resultSize = Array2D.length1 matrix
        let resultVector = Array.zeroCreate<'a> resultSize

        let times = semiring.Times.Invoke
        let plus = semiring.Plus.Invoke

        let task i =
            let col = matrix.[i, *]
            resultVector.[i] <-
                vector
                |> Array.Parallel.mapi (fun i v -> times v col.[i])
                |> Array.fold (fun x y -> plus x y) semiring.Zero

        System.Threading.Tasks.Parallel.For(0, resultSize, task) |> ignore

        resultVector
        |> Seq.cast<'a>
        |> Seq.mapi (fun i v -> (i, v))
        |> Seq.filter
            (fun (i, v) ->
                not (isZero v) &&
                match case.MaskCase with
                | NoMask -> true
                | Regular -> mask.[i]
                | Complemented -> not mask.[i]
            )
        |> Array.ofSeq
        |> Array.unzip
        |> fun (cols, vals) ->
            {
                Indices = cols
                Values = vals
            }

    let actual =
        try
            let matrix = Utils.createMatrixFromArray2D case.MatrixCase matrix isZero
            let vector = Utils.createVectorFromArray case.VectorCase vector isZero
            let mask = Utils.createVectorFromArray VectorFormat.COO mask not

            logger.debug (
                eventX "Matrix is \n{matrix}"
                >> setField "matrix" matrix
            )

            logger.debug (
                eventX "Vector is \n{vector}"
                >> setField "vector" vector
            )

            graphblas {
                let! result =
                    match case.MaskCase with
                    | NoMask -> Matrix.mxv semiring matrix vector
                    | Regular ->
                        Vector.mask mask
                        >>= fun mask -> Matrix.mxvWithMask semiring mask matrix vector
                    | Complemented ->
                        Vector.complemented mask
                        >>= fun mask -> Matrix.mxvWithMask semiring mask matrix vector

                let! tuples = Vector.tuples result
                do! VectorTuples.synchronize tuples
                return tuples
            }
            |> EvalGB.withClContext case.ClContext
            |> EvalGB.runSync

        finally
            case.ClContext.Provider.CloseAllBuffers()

    logger.debug (
        eventX "Expected result is {expected}"
        >> setField "expected" (sprintf "%A" expected.Values)
    )

    logger.debug (
        eventX "Actual result is {actual}"
        >> setField "actual" (sprintf "%A" actual.Values)
    )

    let actualIndices = actual.Indices
    let expectedIndices = expected.Indices

    "Indices of expected and result vector must be the same"
    |> Expect.sequenceEqual actualIndices expectedIndices

    let equality =
        (expected.Values, actual.Values)
        ||> Seq.map2 isEqual

    "Length of expected and result values should be equal"
    |> Expect.hasLength actual.Values (Seq.length expected.Values)

    "There should be no difference between expected and received values"
    |> Expect.allEqual equality true

let testFixtures case = [
    let config = Utils.defaultConfig

    let getTestName datatype =
        sprintf "Correctness on %s, %A, %A, %A, %O"
            datatype
            case.MatrixCase
            case.VectorCase
            case.MaskCase
            case.ClContext

    case
    |> checkCorrectnessGeneric<int> AddMult.int (=)
    |> testPropertyWithConfig config (getTestName "int")

    case
    |> checkCorrectnessGeneric<float> AddMult.float (fun x y -> abs (x - y) < Accuracy.medium.relative)
    |> testPropertyWithConfig config (getTestName "float")

    case
    |> checkCorrectnessGeneric<sbyte> AddMult.sbyte (=)
    |> ptestPropertyWithConfig config (getTestName "sbyte")

    case
    |> checkCorrectnessGeneric<byte> AddMult.byte (=)
    |> ptestPropertyWithConfig config (getTestName "byte")

    case
    |> checkCorrectnessGeneric<int16> AddMult.int16 (=)
    |> testPropertyWithConfig config (getTestName "int16")

    case
    |> checkCorrectnessGeneric<uint16> AddMult.uint16 (=)
    |> testPropertyWithConfig config (getTestName "uint16")

    case
    |> checkCorrectnessGeneric<bool> AnyAll.bool (=)
    |> testPropertyWithConfig config (getTestName "bool")
]

let tests =
    testCases
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.Device
            let deviceType = Cl.GetDeviceInfo(device, DeviceInfo.Type, &e).CastTo<DeviceType>()

            deviceType = DeviceType.Cpu &&
            case.VectorCase = VectorFormat.COO &&
            case.MatrixCase = CSR &&
            case.MaskCase = NoMask
        )
    |> List.collect testFixtures
    |> testList "Mxv tests"
