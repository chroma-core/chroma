// Assumes that chroma-hnswlib is checked out at the same level as chroma
#include "../../../hnswlib/hnswlib/hnswlib.h"
#include <thread>

class AllowAndDisallowListFilterFunctor : public hnswlib::BaseFilterFunctor
{
public:
    std::unordered_set<hnswlib::labeltype> allow_list;
    std::unordered_set<hnswlib::labeltype> disallow_list;

    AllowAndDisallowListFilterFunctor(std::unordered_set<hnswlib::labeltype> allow_list, std::unordered_set<hnswlib::labeltype> disallow_list) : allow_list(allow_list), disallow_list(disallow_list) {}

    bool operator()(hnswlib::labeltype id)
    {
        if (allow_list.size() > 0 && allow_list.find(id) == allow_list.end())
        {
            return false;
        }
        if (disallow_list.size() > 0 && disallow_list.find(id) != disallow_list.end())
        {
            return false;
        }
        return true;
    }
};

// thread-local for the last error message, callers are expected to check this
// the empty string represents no error
// this is currently shared across all instances of Index, but that's fine for now
// since it is thread-local
thread_local std::string last_error;

template <typename dist_t, typename data_t = float>
class Index
{
public:
    std::string space_name;
    int dim;
    size_t seed;

    bool normalize;
    bool index_inited;

    hnswlib::HierarchicalNSW<dist_t> *appr_alg;
    hnswlib::SpaceInterface<float> *l2space;

    Index(const std::string &space_name, const int dim) : space_name(space_name), dim(dim)
    {
        if (space_name == "l2")
        {
            l2space = new hnswlib::L2Space(dim);
            normalize = false;
        }
        if (space_name == "ip")
        {
            l2space = new hnswlib::InnerProductSpace(dim);
            // For IP, we expect the vectors to be normalized
            normalize = false;
        }
        if (space_name == "cosine")
        {
            l2space = new hnswlib::InnerProductSpace(dim);
            normalize = true;
        }
        appr_alg = NULL;
        index_inited = false;
        last_error.clear();
    }

    ~Index()
    {
        delete l2space;
        if (appr_alg)
        {
            delete appr_alg;
        }
    }

    void init_index(const size_t max_elements, const size_t M, const size_t ef_construction, const size_t random_seed, const bool allow_replace_deleted, const bool is_persistent_index, const std::string &persistence_location)
    {
        if (index_inited)
        {
            throw std::runtime_error("Index already inited");
        }
        appr_alg = new hnswlib::HierarchicalNSW<dist_t>(l2space, max_elements, M, ef_construction, random_seed, allow_replace_deleted, normalize, is_persistent_index, persistence_location);
        appr_alg->ef_ = 10; // This is a default value for ef_
        index_inited = true;
    }

    void load_index(const std::string &path_to_index, const bool allow_replace_deleted, const bool is_persistent_index)
    {
        if (index_inited)
        {
            throw std::runtime_error("Index already inited");
        }
        appr_alg = new hnswlib::HierarchicalNSW<dist_t>(l2space, path_to_index, false, 0, allow_replace_deleted, normalize, is_persistent_index);
        appr_alg->checkIntegrity();
        index_inited = true;
    }

    void persist_dirty()
    {
        if (!index_inited)
        {
            throw std::runtime_error("Index not inited");
        }
        appr_alg->persistDirty();
    }

    void add_item(const data_t *data, const hnswlib::labeltype id, const bool replace_deleted = false)
    {
        if (!index_inited)
        {
            throw std::runtime_error("Index not inited");
        }

        appr_alg->addPoint(data, id);
    }

    void get_item(const hnswlib::labeltype id, data_t *data)
    {
        if (!index_inited)
        {
            throw std::runtime_error("Inde not inited");
        }
        std::vector<data_t> ret_data = appr_alg->template getDataByLabel<data_t>(id); // This checks if id is deleted
        for (int i = 0; i < dim; i++)
        {
            data[i] = ret_data[i];
        }
    }

    void mark_deleted(const hnswlib::labeltype id)
    {
        if (!index_inited)
        {
            throw std::runtime_error("Index not inited");
        }
        appr_alg->markDelete(id);
    }

    size_t knn_query(const data_t *query_vector, const size_t k, hnswlib::labeltype *ids, data_t *distance, const hnswlib::labeltype *allowed_ids, const size_t allowed_id_length, const hnswlib::labeltype *disallowed_ids, const size_t disallowed_id_length)
    {
        if (!index_inited)
        {
            throw std::runtime_error("Index not inited");
        }

        std::unordered_set<hnswlib::labeltype> allow_list;
        std::unordered_set<hnswlib::labeltype> disallow_list;
        if (allowed_ids != NULL)
        {
            for (int i = 0; i < allowed_id_length; i++)
            {
                allow_list.insert(allowed_ids[i]);
            }
        }
        if (disallowed_ids != NULL)
        {
            for (int i = 0; i < disallowed_id_length; i++)
            {
                disallow_list.insert(disallowed_ids[i]);
            }
        }
        AllowAndDisallowListFilterFunctor filter = AllowAndDisallowListFilterFunctor(allow_list, disallow_list);
        std::priority_queue<std::pair<dist_t, hnswlib::labeltype>> res = appr_alg->searchKnn(query_vector, k, &filter);
        int total_results = std::min(res.size(), k);
        for (int i = total_results - 1; i >= 0; i--)
        {
            std::pair<dist_t, hnswlib::labeltype> res_i = res.top();
            ids[i] = res_i.second;
            distance[i] = res_i.first;
            res.pop();
        }
        return total_results;
    }

    int get_ef()
    {
        if (!index_inited)
        {
            throw std::runtime_error("Index not inited");
        }
        return appr_alg->ef_;
    }

    void set_ef(const size_t ef)
    {
        if (!index_inited)
        {
            throw std::runtime_error("Index not inited");
        }
        appr_alg->ef_ = ef;
    }

    void resize_index(size_t new_size)
    {
        if (!index_inited)
        {
            throw std::runtime_error("Index not inited");
        }
        appr_alg->resizeIndex(new_size);
    }
};

// All these methods except for len() and capacity() can "throw" a std::exception
// and populate the last_error thread-local variable. This is how we communicate
// errors across the FFI boundary - the C++ layer will catch all exceptions and
// set the last_error variable, which the Rust layer can then check.
// Comments referring to "throwing" exceptions in this block refer to this mechanism.
extern "C"
{

    // Can throw std::exception
    Index<float> *create_index(const char *space_name, const int dim)
    {
        Index<float> *index;
        try
        {
            index = new Index<float>(space_name, dim);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return nullptr;
        }
        last_error.clear();
        return new Index<float>(space_name, dim);
    }

    void free_index(Index<float> *index)
    {
        delete index;
    }

    // Can throw std::exception
    void init_index(Index<float> *index, const size_t max_elements, const size_t M, const size_t ef_construction, const size_t random_seed, const bool allow_replace_deleted, const bool is_persistent_index, const char *persistence_location)
    {
        try
        {
            index->init_index(max_elements, M, ef_construction, random_seed, allow_replace_deleted, is_persistent_index, persistence_location);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    // Can throw std::exception
    void load_index(Index<float> *index, const char *path_to_index, const bool allow_replace_deleted, const bool is_persistent_index)
    {
        try
        {
            index->load_index(path_to_index, allow_replace_deleted, is_persistent_index);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    // Can throw std::exception
    void persist_dirty(Index<float> *index)
    {
        try
        {
            index->persist_dirty();
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    // Can throw std::exception
    void add_item(Index<float> *index, const float *data, const hnswlib::labeltype id, const bool replace_deleted)
    {
        try
        {
            index->add_item(data, id, replace_deleted);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    // Can throw std::exception
    void get_item(Index<float> *index, const hnswlib::labeltype id, float *data)
    {
        try
        {
            index->get_item(id, data);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    // Can throw std::exception
    void mark_deleted(Index<float> *index, const hnswlib::labeltype id)
    {
        try
        {
            index->mark_deleted(id);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    // Can throw std::exception
    size_t knn_query(Index<float> *index, const float *query_vector, const size_t k, hnswlib::labeltype *ids, float *distance, const hnswlib::labeltype *allowed_ids, const size_t allowed_id_length, const hnswlib::labeltype *disallowed_ids, const size_t disallowed_id_length)
    {
        size_t result;
        try
        {
            result = index->knn_query(query_vector, k, ids, distance, allowed_ids, allowed_id_length, disallowed_ids, disallowed_id_length);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return 0;
        }
        last_error.clear();
        return result;
    }

    // Can throw std::exception
    int get_ef(Index<float> *index)
    {
        int ret;
        try
        {
            ret = index->get_ef();
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return -1;
        }
        last_error.clear();
        return ret;
    }

    // Can throw std::exception
    void set_ef(Index<float> *index, const size_t ef)
    {
        try
        {
            index->set_ef(ef);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    // Can not throw std::exception
    int len(Index<float> *index)
    {
        if (!index->index_inited)
        {
            return 0;
        }

        return index->appr_alg->getCurrentElementCount() - index->appr_alg->getDeletedCount();
    }

    // Can not throw std::exception
    size_t capacity(Index<float> *index)
    {
        if (!index->index_inited)
        {
            return 0;
        }

        return index->appr_alg->max_elements_;
    }

    // Can throw std::exception
    void resize_index(Index<float> *index, size_t new_size)
    {
        try
        {
            index->resize_index(new_size);
        }
        catch (std::exception &e)
        {
            last_error = e.what();
            return;
        }
        last_error.clear();
    }

    const char *get_last_error(Index<float> *index)
    {
        if (last_error.empty())
        {
            return nullptr;
        }
        return last_error.c_str();
    }
}
