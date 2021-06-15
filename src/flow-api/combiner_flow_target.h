/**
 * @file combiner_flow_target.h
 * @author lthostrup
 * @date 2019-08-01
 */

#pragma once

#include "../utils/Config.h"
#include "../dfi/registry/RegistryClient.h"
#include "../dfi/type/ErrorCodes.h"
#if defined(__AVX512F__) && defined(__AVX512BW__)
#include <immintrin.h>
#endif
class DFI_Combiner_flow_target
{
public:
    /**
     * @brief Construct a new dfi combiner flow target object
     * 
     * @param flowName Unique flow name identifier
     * @param targetId Target identifier - Must correspond to TargetID passed to flow initialization
     * @param consumeScheme - Scheme determining if segments can be consumed asyncronous/syncronous across Flow-sources.
                            * Note: if optimization goal is BW, multiple tuples potentially fit into one segment and will 
                            * always be consumed and aggregated together before next segment is accessed.
     */
    DFI_Combiner_flow_target(string flowName, TargetID targetId, ConsumeScheme consumeScheme = ConsumeScheme::ASYNC) : consumeScheme(consumeScheme)
    {
        //Initialize FlowHandle
        dfi::RegistryClient regClient;
        while((m_flowHandle = regClient.retrieveFlowHandle(flowName)) == nullptr)
        {
            usleep(Config::DFI_SLEEP_INTERVAL);
        }
        m_schema = &m_flowHandle->schema;

        if (m_flowHandle->flowtype != FlowType::COMBINER)
        {
            Logging::error(__FILE__, __LINE__, "DFI_Combiner_flow_source expected a flowhandle with combiner type, but received: " + to_string(m_flowHandle->flowtype));
            return;
        }

        //Initialize BufferIteratorBW
        std::unique_ptr<BufferHandle> bufferHandle = nullptr;
        auto bufferName = Config::getBufferName(m_flowHandle->name, targetId);
        while((bufferHandle = regClient.retrieveBuffer(bufferName, m_flowHandle->name)) == nullptr)
        {
            usleep(Config::DFI_SLEEP_INTERVAL);
        }
        
        bufferIterator = bufferHandle->getNewIterator(consumeScheme);

        Logging::debug(__FILE__, __LINE__, "DFI_Combiner_flow_target: Initialized for flow: " + m_flowHandle->name + ", target: " + to_string(targetId));
        num_col = m_flowHandle->schema.getColumnCount();
        newTuple = dfi::Tuple(&m_flowHandle->schema);
        initTuple(refTuple);

        //Use SIMD decision
        m_useSimd = false;
#if defined(__AVX512F__) && defined(__AVX512BW__)
        auto num_col = m_schema->getColumnCount();
        auto colSize = Type::getTypeSize(m_schema->getTypeForColumn(0));
        if (colSize == 2 && num_col >= 32 && (num_col % 32) == 0)
            m_useSimd = true;
        else if (colSize == 4 && num_col >= 16 && (num_col % 16) == 0)
            m_useSimd = true;
        else if (colSize == 8 && num_col >= 8 && (num_col % 8) == 0)
            m_useSimd = true;
        
        for (size_t i = 1; i < num_col; i++)
        {
            if (m_schema->getTypeForColumn(i) != m_schema->getTypeForColumn(i-1))
                m_useSimd = false;
        }
#endif
    if (m_useSimd)
        Logging::info("DFI_Combiner_flow_target using SIMD");

    }

    ~DFI_Combiner_flow_target()
    {
        if (bufferIterator != nullptr)
        {
            delete bufferIterator;
        }
    }


    /**
     * @brief Consumes (potentially multiple) tuples out of the combiner flow. 
     * 
     * @param retTuple - The combined (aggregated) tuple to be returned.
     * @param tuplesToConsume - Number of tuples to consume and aggregate before returning the aggregated tuple (retTuple).
     * @param retTuplesConsumed - Number of exact tuples that were consumed. Is always bigger or equal to tuplesToConsume, except for case where flow has ended.
     * @return int - Status code
     */
    int consume(dfi::Tuple &retTuple, size_t tuplesToConsume, size_t &retTuplesConsumed)
    {
        retTuplesConsumed = 0;
        retTuple = create_tuple();
        BufferIterator::HasNextReturn hasNext;
        while(retTuplesConsumed < tuplesToConsume)
        {
            hasNext = bufferIterator->has_next();
            if (hasNext == BufferIterator::HasNextReturn::BUFFER_CLOSED)
            {
                if (retTuplesConsumed > 0)
                    return DFI_SUCCESS;
                else
                    return DFI_FLOW_FINISHED;
            }
            else if (hasNext == BufferIterator::HasNextReturn::TRUE)
            {
                size_t size = 0;
                auto dataPtr = bufferIterator->next(size);
                newTuple.setDataPtr(dataPtr);
                size_t tuplesConsumed = size / m_schema->getTupleSize();
                if (tuplesConsumed == 1)
                    aggregate(retTuple, newTuple, ++retTuplesConsumed);
                else
                {
                    for (size_t i = 0; i < tuplesConsumed; i++)
                    {
                        aggregate(retTuple, newTuple, ++retTuplesConsumed);
                    }
                }
                
                // std::cout << "Buffer has next. retTuplesConsumed: " << retTuplesConsumed << " tuplesToConsume: " << tuplesToConsume << std::endl;
            }
        }
        return DFI_SUCCESS;
    }

    /**
     * @brief Get the DFI schema
     * 
     * @param schema reference sat by function
     * @return DFI return code
     */
    int get_schema(dfi::Schema &schema)
    {
        schema = m_flowHandle->schema;
        return DFI_SUCCESS;
    }
    
    /**
     * @brief Create a DFI tuple instantiated with schema
     * 
     * @return dfi::Tuple 
     */
    dfi::Tuple create_tuple()
    {
        dfi::Tuple tuple(&m_flowHandle->schema);
        initTuple(tuple);
        return tuple;
    }

private:

    std::unique_ptr<dfi::FlowHandle> m_flowHandle = nullptr;
    ConsumeScheme consumeScheme;
    BufferIterator *bufferIterator = nullptr;
    Schema *m_schema = nullptr;
    Tuple newTuple;
    Tuple refTuple;
    size_t num_col;
    bool m_useSimd = false;

    // Initializes the tuple values to 0 to store the aggregate
    void initTuple(dfi::Tuple &tuple)
    {
        tuple.setSchema(&m_flowHandle->schema);
        tuple.allocateTupleMemory();
        for(size_t i = 0; i < m_flowHandle->schema.getColumnCount(); i++)
        {
            tuple.setValue(i, Value::getDefaultValue(m_flowHandle->schema.getTypeForColumn(i)));
        }
    }

    void aggregate(dfi::Tuple &tuple, dfi::Tuple &newTuple, size_t tuplesConsumed)
    {
        switch (m_flowHandle->aggrFunc)
        {
            case AggrFunc::SUM:
#if defined(__AVX512F__) && defined(__AVX512BW__)
                if (m_useSimd)
                {
                    if (m_schema->getTypeForColumn(0) == TypeId::DOUBLE)
                        simd_op_double<AggrFunc::SUM>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::FLOAT)
                        simd_op_float<AggrFunc::SUM>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::INT)
                        simd_op_int<AggrFunc::SUM>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::TINYINT)
                        simd_op_tinyint<AggrFunc::SUM>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::BIGINT)
                        simd_op_bigint<AggrFunc::SUM>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::BIGUINT)
                        simd_op_biguint<AggrFunc::SUM>(tuple, newTuple, num_col);
                    return;
                }
#endif
                for(size_t i = 0; i < num_col; i++)
                    tuple.setValue(i, tuple.getValue(i) + newTuple.getValue(i));
                return;
            case AggrFunc::MEAN:
            {
#if defined(__AVX512F__) && defined(__AVX512BW__)
                if (m_useSimd)
                {
                    if (m_schema->getTypeForColumn(0) == TypeId::DOUBLE)
                        simd_op_double<AggrFunc::MEAN>(tuple, newTuple, num_col, tuplesConsumed);
                    else if (m_schema->getTypeForColumn(0) == TypeId::FLOAT)
                        simd_op_float<AggrFunc::MEAN>(tuple, newTuple, num_col, tuplesConsumed);
                    return;
                }
#endif
                for(size_t i = 0; i < num_col; i++)
                {
                    auto newVal = tuple.getValue(i);
                    tuple.setValue(i, newVal + ((newTuple.getValue(i) - newVal) / tuplesConsumed));
                }
                return;
            }
            case AggrFunc::MULT:
#if defined(__AVX512F__) && defined(__AVX512BW__)
                if (m_useSimd)
                {
                    if (m_schema->getTypeForColumn(0) == TypeId::DOUBLE)
                        simd_op_double<AggrFunc::MULT>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::FLOAT)
                        simd_op_float<AggrFunc::MULT>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::INT)
                        simd_op_int<AggrFunc::MULT>(tuple, newTuple, num_col);
                    return;
                }
#endif
                for(size_t i = 0; i < num_col; i++)
                    tuple.setValue(i, tuple.getValue(i) * newTuple.getValue(i));
                return;
            case AggrFunc::MAX:
#if defined(__AVX512F__) && defined(__AVX512BW__)
                if (m_useSimd)
                {
                    if (m_schema->getTypeForColumn(0) == TypeId::DOUBLE)
                        simd_op_double<AggrFunc::MAX>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::FLOAT)
                        simd_op_float<AggrFunc::MAX>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::INT)
                        simd_op_int<AggrFunc::MAX>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::TINYINT)
                        simd_op_tinyint<AggrFunc::MAX>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::BIGINT)
                        simd_op_bigint<AggrFunc::MAX>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::BIGUINT)
                        simd_op_biguint<AggrFunc::MAX>(tuple, newTuple, num_col);
                    return;
                }
#endif
                for(size_t i = 0; i < num_col; i++)
                    tuple.setValue(i, Value::max(tuple.getValue(i), newTuple.getValue(i)));
                return;
            case AggrFunc::MIN: 
#if defined(__AVX512F__) && defined(__AVX512BW__)
                if (m_useSimd)
                {
                    if (m_schema->getTypeForColumn(0) == TypeId::DOUBLE)
                        simd_op_double<AggrFunc::MIN>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::FLOAT)
                        simd_op_float<AggrFunc::MIN>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::INT)
                        simd_op_int<AggrFunc::MIN>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::TINYINT)
                        simd_op_tinyint<AggrFunc::MIN>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::BIGINT)
                        simd_op_bigint<AggrFunc::MIN>(tuple, newTuple, num_col);
                    else if (m_schema->getTypeForColumn(0) == TypeId::BIGUINT)
                        simd_op_biguint<AggrFunc::MIN>(tuple, newTuple, num_col);
                    return;
                }
#endif
                for(size_t i = 0; i < num_col; i++)
                    tuple.setValue(i, Value::min(tuple.getValue(i), newTuple.getValue(i)));
                return;
            default:
                return;
        }
    }


#if defined(__AVX512F__) && defined(__AVX512BW__)
        
    template <AggrFunc aggrFunc>
    inline void simd_op_float(Tuple &tuple, Tuple &newTuple, size_t num_col, size_t tuplesConsumed = 0)
    {
        __m512 result;
        for(size_t i = 0; i < num_col/16; i++)
        {
            __m512 newVals = _mm512_loadu_ps(newTuple.getDataPtr(i*16));
            __m512 oldVals = _mm512_loadu_ps(tuple.getDataPtr(i*16));
            switch (aggrFunc)
            {
                case AggrFunc::SUM:
                    result = _mm512_add_ps(newVals, oldVals); 
                    break;
                case AggrFunc::SUB:
                    result = _mm512_sub_ps(newVals, oldVals);
                    break;
                case AggrFunc::MEAN:
                {
                    __m512 sub1 = _mm512_sub_ps(newVals, oldVals);
                    __m512 div1 = _mm512_div_ps(sub1, _mm512_set1_ps(tuplesConsumed));
                    result = _mm512_add_ps(div1, oldVals);
                    break;
                }
                case AggrFunc::MULT:
                    result = _mm512_mul_ps(newVals, oldVals);
                    break;
                case AggrFunc::MAX:
                    result = _mm512_max_ps(newVals, oldVals);
                    break;
                case AggrFunc::MIN:
                    result = _mm512_min_ps(newVals, oldVals);
                    break;
                
            }
            _mm512_storeu_ps(tuple.getDataPtr(i*16), result);
        }
    }
    
    template <AggrFunc aggrFunc>
    inline void simd_op_double(Tuple &tuple, Tuple &newTuple, size_t num_col, size_t tuplesConsumed = 0)
    {
        __m512d result;
        for(size_t i = 0; i < num_col/8; i++)
        {
            __m512d newVals = _mm512_loadu_pd(newTuple.getDataPtr(i*8));
            __m512d oldVals = _mm512_loadu_pd(tuple.getDataPtr(i*8));
            switch (aggrFunc)
            {
                case AggrFunc::SUM:
                    result = _mm512_add_pd(newVals, oldVals); 
                    break;
                case AggrFunc::SUB:
                    result = _mm512_sub_pd(newVals, oldVals);
                    break;
                case AggrFunc::MEAN:
                {
                    __m512d sub1 = _mm512_sub_pd(newVals, oldVals);
                    __m512d div1 = _mm512_div_pd(sub1, _mm512_set1_pd(tuplesConsumed));
                    result = _mm512_add_pd(div1, oldVals);
                    break;
                }
                case AggrFunc::MULT:
                    result = _mm512_mul_pd(newVals, oldVals);
                    break;
                case AggrFunc::MAX:
                    result = _mm512_max_pd(newVals, oldVals);
                    break;
                case AggrFunc::MIN:
                    result = _mm512_min_pd(newVals, oldVals);
                    break;
                
            }
            _mm512_storeu_pd(tuple.getDataPtr(i*8), result);
        }
    }


    template <AggrFunc aggrFunc>
    inline void simd_op_tinyint(Tuple &tuple, Tuple &newTuple, size_t num_col)
    {
        __m512i result;
        for(size_t i = 0; i < num_col/32; i++)
        {
            __m512i newVals = _mm512_loadu_si512((void*)newTuple.getDataPtr(i*32));
            __m512i oldVals = _mm512_loadu_si512(tuple.getDataPtr(i*32));
            switch (aggrFunc)
            {
                case AggrFunc::SUM:
                    result = _mm512_add_epi16(newVals, oldVals); 
                    break;
                case AggrFunc::SUB:
                    result = _mm512_sub_epi16(newVals, oldVals);
                    break;
                case AggrFunc::MAX:
                    result = _mm512_max_epi16(newVals, oldVals);
                    break;
                case AggrFunc::MIN:
                    result = _mm512_min_epi16(newVals, oldVals);
                    break;
                default:
                    throw new std::runtime_error("Combiner flow target could not perform requested simd aggretion function!");

            }
            _mm512_storeu_si512(tuple.getDataPtr(i*32), result);
        }
    }


    template <AggrFunc aggrFunc>
    inline void simd_op_int(Tuple &tuple, Tuple &newTuple, size_t num_col)
    {
        __m512i result;
        for(size_t i = 0; i < num_col/16; i++)
        {
            __m512i newVals = _mm512_loadu_si512(newTuple.getDataPtr(i*16));
            __m512i oldVals = _mm512_loadu_si512(tuple.getDataPtr(i*16));
            switch (aggrFunc)
            {
                case AggrFunc::SUM:
                    result = _mm512_add_epi32(newVals, oldVals); 
                    break;
                case AggrFunc::SUB:
                    result = _mm512_sub_epi32(newVals, oldVals);
                    break;
                case AggrFunc::MULT:
                    result = _mm512_mul_epi32(newVals, oldVals);
                    break;
                case AggrFunc::MAX:
                    result = _mm512_max_epi32(newVals, oldVals);
                    break;
                case AggrFunc::MIN:
                    result = _mm512_min_epi32(newVals, oldVals);
                    break;
                default:
                    throw new std::runtime_error("Combiner flow target could not perform requested simd aggretion function!");
            }
            _mm512_storeu_si512(tuple.getDataPtr(i*16), result);
        }
    }


    template <AggrFunc aggrFunc>
    inline void simd_op_bigint(Tuple &tuple, Tuple &newTuple, size_t num_col)
    {
        __m512i result;
        for(size_t i = 0; i < num_col/8; i++)
        {
            __m512i newVals = _mm512_loadu_si512(newTuple.getDataPtr(i*8));
            __m512i oldVals = _mm512_loadu_si512(tuple.getDataPtr(i*8));
            switch (aggrFunc)
            {
                case AggrFunc::SUM:
                    result = _mm512_add_epi64(newVals, oldVals); 
                    break;
                case AggrFunc::SUB:
                    result = _mm512_sub_epi64(newVals, oldVals);
                    break;
                case AggrFunc::MAX:
                    result = _mm512_max_epi64(newVals, oldVals);
                    break;
                case AggrFunc::MIN:
                    result = _mm512_min_epi64(newVals, oldVals);
                    break;
                default:
                    throw new std::runtime_error("Combiner flow target could not perform requested simd aggretion function!");
            }
            _mm512_storeu_si512(tuple.getDataPtr(i*8), result);
        }
    }

    template <AggrFunc aggrFunc>
    inline void simd_op_biguint(Tuple &tuple, Tuple &newTuple, size_t num_col)
    {
        __m512i result;
        for(size_t i = 0; i < num_col/8; i++)
        {
            __m512i newVals = _mm512_loadu_si512(newTuple.getDataPtr(i*8));
            __m512i oldVals = _mm512_loadu_si512(tuple.getDataPtr(i*8));
            switch (aggrFunc)
            {
                case AggrFunc::SUM:
                    result = _mm512_add_epi64(newVals, oldVals); 
                    break;
                case AggrFunc::SUB:
                    result = _mm512_sub_epi64(newVals, oldVals);
                    break;
                case AggrFunc::MAX:
                    result = _mm512_max_epu64(newVals, oldVals);
                    break;
                case AggrFunc::MIN:
                    result = _mm512_min_epu64(newVals, oldVals);
                    break;
                default:
                    throw new std::runtime_error("Combiner flow target could not perform requested simd aggretion function!");
            }
            _mm512_storeu_si512(tuple.getDataPtr(i*8), result);
        }
    }

#endif
};
