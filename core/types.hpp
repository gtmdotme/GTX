//
// Created by zhou822 on 5/22/23.
//

#ifndef BWGRAPH_V2_TYPES_HPP
#define BWGRAPH_V2_TYPES_HPP
#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>

namespace bwgraph
{
    using label_t = uint16_t;
    using vertex_t = uint64_t;
    using order_t = uint8_t;
    using timestamp_t = uint64_t;

    enum class VertexDeltaType : uint8_t
    {
        // TODO: need to be revisited
        BASE, // for consolidated vertex result
        INSERT_DELTA,
        DELETE_DELTA,
        UPDATE_DELTA,
        NEW_VERTEX // for creating a new vertex
    };

    enum class EdgeDeltaType:int64_t{
        BASE, //for consolidated edge entries
        INSERT_DELTA,//for new edges
        DELETE_DELTA,//for deleting an edge
        UPDATE_DELTA//for updating an edge property
    };

    enum class Delta_Chain_Lock_Response:uint8_t {
        SUCCESS,
        LOCK_INHERIT,
        DEADLOCK,
        CONFLICT,
        UNCLEAR
    };
    enum class Vertex_Index_Entry_State:uint8_t {
        FREE,
        ACTIVE,
        DELETED
    };
    enum class EdgeDeltaBlockState:uint8_t{
        NORMAL,
        OVERFLOW,
        CONSOLIDATION,
       // VALIDATION,
        INSTALLATION
    };
    enum class EdgeDeltaInstallResult:uint8_t{
        SUCCESS,
        CAUSE_OVERFLOW,
        ALREADY_OVERFLOW
    };
}
#endif //BWGRAPH_V2_TYPES_HPP
