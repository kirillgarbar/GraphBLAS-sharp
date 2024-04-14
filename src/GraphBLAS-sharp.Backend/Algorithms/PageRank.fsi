namespace GraphBLAS.FSharp.Backend.Algorithms

open Brahma.FSharp
open GraphBLAS.FSharp.Objects

[<RequireQualifiedAccess>]
module PageRank =
    [<Sealed>]
    type PageRankMatrix =
        member Dispose : DeviceCommandQueue<Msg> -> unit

    val internal prepareMatrix : ClContext -> int -> (DeviceCommandQueue<Msg> -> ClMatrix<float32> -> PageRankMatrix)

    val internal run : ClContext -> int -> (DeviceCommandQueue<Msg> -> PageRankMatrix -> float32 -> ClVector<float32>)
