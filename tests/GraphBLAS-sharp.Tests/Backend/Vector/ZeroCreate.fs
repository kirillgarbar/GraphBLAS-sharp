module GraphBLAS.FSharp.Tests.Backend.Vector.ZeroCreate

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open Context
open TestCases
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open Brahma.FSharp

let logger = Log.create "Vector.zeroCreate.Tests"

let config = Utils.defaultConfig

let wgSize = Constants.Common.defaultWorkGroupSize

let checkResult size (actual: Vector<'a>) =
    Expect.equal actual.Size size "The size should be the same"

    match actual with
    | Vector.Dense vector ->
        Array.iter
        <| (fun item -> Expect.equal item None "values must be None")
        <| vector
    | Vector.Sparse vector ->
        Expect.equal vector.Values [| Unchecked.defaultof<'a> |] "The values array must contain the default value"
        Expect.equal vector.Indices [| 0 |] "The index array must contain the 0"

let correctnessGenericTest<'a when 'a: struct and 'a: equality>
    (zeroCreate: RawCommandQueue -> AllocationFlag -> int -> VectorFormat -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (vectorSize: int)
    =

    let vectorSize = abs vectorSize

    if vectorSize > 0 then
        try
            let q = case.TestContext.Queue

            let clVector =
                zeroCreate q DeviceOnly vectorSize case.Format

            let hostVector = clVector.ToHost q

            clVector.Dispose()

            checkResult vectorSize hostVector
        with
        | ex when ex.Message = "Attempting to create full sparse vector" -> ()
        | ex -> raise ex

let createTest<'a> case =
    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.Format}"

    let context = case.TestContext.ClContext

    let intZeroCreate = Vector.zeroCreate context wgSize

    case
    |> correctnessGenericTest<int> intZeroCreate
    |> testPropertyWithConfig config (getCorrectnessTestName $"%A{typeof<'a>}")

let testFixtures case =
    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    //q.Error.Add(fun e -> failwithf "%A" e)

    [ createTest<int> case
      createTest<byte> case

      if Utils.isFloat64Available context.ClDevice then
          createTest<float> case

      createTest<float32> case
      createTest<bool> case ]

let tests =
    operationGPUTests "Backend.Vector.zeroCreate tests" testFixtures
