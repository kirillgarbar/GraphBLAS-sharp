namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open System.Text.RegularExpressions
open GraphBLAS.FSharp
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp.OpenCL
open OpenCL.Net

type Config1() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", (fun (matrix,_) -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun (matrix,_) -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn(
                "NNZ",
                fun (matrix,_) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            MatrixShapeColumn(
                "SqrNNZ",
                fun (_,matrix) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config1>)>]
type TestBenchmarks<'matrixResT, 'matrixInT, 'elem when 'matrixResT :> Backend.IDeviceMemObject and 'matrixInT :> Backend.IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<'matrixInT>
    let mutable secondMatrix = Unchecked.defaultof<'matrixInT>
    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>

    member val ResultMatrix = Unchecked.defaultof<'matrixResT> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<_> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader*MtxReader> with get, set

    member this.OclContext:ClContext = fst this.OclContextInfo
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = this.OclContext.CommandQueue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    static member InputMatricesProviderBuilder pathToConfig =
        let datasetFolder = "EWiseAdd"
        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" ->
                    MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename)
                    , MtxReader(Utils.getFullPathToMatrix datasetFolder ("squared_" + matrixFilename))
                | _ -> failwith "Unsupported matrix format")

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.ReadMatrix (reader:MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.PerformBenchmark() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix

    member this.ClearInputMatrices() =
        (firstMatrix :> Backend.IDeviceMemObject).Dispose()
        (secondMatrix :> Backend.IDeviceMemObject).Dispose()

    member this.ClearResult() =
        (this.ResultMatrix :> Backend.IDeviceMemObject).Dispose()

    member this.ReadMatrices() =
        let leftMatrixReader = fst this.InputMatrixReader
        let rightMatrixReader = snd this.InputMatrixReader
        firstMatrixHost <- this.ReadMatrix leftMatrixReader
        secondMatrixHost <- this.ReadMatrix rightMatrixReader

    member this.LoadMatricesToGPU () =
        firstMatrix <- buildMatrix this.OclContext firstMatrixHost
        secondMatrix <- buildMatrix this.OclContext secondMatrixHost

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type ConvertBenchmarks<'matrixResT, 'matrixInT, 'elem when 'matrixResT :> Backend.IDeviceMemObject and 'matrixInT :> Backend.IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit TestBenchmarks<'matrixResT, 'matrixInT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrices ()
        this.LoadMatricesToGPU ()

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup () =
        this.ClearInputMatrices()

    [<Benchmark>]
    override this.Benchmark () =
        this.PerformBenchmark()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

type ConvertBenchmarksToCSRBool() =

    inherit ConvertBenchmarks<Backend.CSRMatrix<bool>, Backend.COOMatrix<bool>, bool>(
        (fun context wgSize -> Backend.COOMatrix.toCSR context wgSize),
        (fun _ -> true),
        (fun _ -> true),
        M.buildCooMatrix
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "ConvertBenchmarks4Bool.txt"

