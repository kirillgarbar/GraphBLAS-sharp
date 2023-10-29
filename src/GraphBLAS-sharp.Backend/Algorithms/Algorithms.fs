namespace GraphBLAS.FSharp

open Microsoft.FSharp.Core
open GraphBLAS.FSharp.Backend.Algorithms

[<RequireQualifiedAccess>]
module Algorithms =
    module BFS =
        let singleSource = BFS.singleSource

        let singleSourceSPLA = BFS.singleSourceSPLA

        let singleSourceSparse = BFS.singleSourceSparse

        let singleSourcePushPull = BFS.singleSourcePushPull

    module SSSP =
        let run = SSSP.run

    module PageRank =
        let run = PageRank.run

        let prepareMatrix = PageRank.prepareMatrix
