namespace GraphBLAS.FSharp.Backend.Algorithms

open Brahma.FSharp
open GraphBLAS.FSharp.Objects

[<RequireQualifiedAccess>]
module PageRank =
    [<Sealed>]
    type PageRankMatrix =
        member Dispose : unit -> unit

    val internal prepareMatrix : ClContext -> int -> (RawCommandQueue -> ClMatrix<float32> -> PageRankMatrix)

    val internal run : ClContext -> int -> (RawCommandQueue -> PageRankMatrix -> float32 -> ClVector<float32>)
