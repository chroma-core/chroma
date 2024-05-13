// Assumes that chroma-hnswlib is checked out at the same level as chroma
#include "../../../hnswlib/hnswlib/hnswlib.h"

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
            std::runtime_error("Index already inited");
        }
        appr_alg = new hnswlib::HierarchicalNSW<dist_t>(l2space, max_elements, M, ef_construction, random_seed, allow_replace_deleted, normalize, is_persistent_index, persistence_location);
        appr_alg->ef_ = 10; // This is a default value for ef_
        index_inited = true;
    }

    void load_index(const std::string &path_to_index, const bool allow_replace_deleted, const bool is_persistent_index)
    {
        if (index_inited)
        {
            std::runtime_error("Index already inited");
        }
        appr_alg = new hnswlib::HierarchicalNSW<dist_t>(l2space, path_to_index, false, 0, allow_replace_deleted, normalize, is_persistent_index);
        index_inited = true;
    }

    void persist_dirty()
    {
        if (!index_inited)
        {
            std::runtime_error("Index not inited");
        }
        appr_alg->persistDirty();
    }

    void add_item(const data_t *data, const hnswlib::labeltype id, const bool replace_deleted = false)
    {
        if (!index_inited)
        {
            std::runtime_error("Index not inited");
        }
        appr_alg->addPoint(data, id);
    }

    void get_item(const hnswlib::labeltype id, data_t *data)
    {
        if (!index_inited)
        {
            std::runtime_error("Index not inited");
        }
        std::vector<data_t> ret_data = appr_alg->template getDataByLabel<data_t>(id); // This checks if id is deleted
        for (int i = 0; i < dim; i++)
        {
            data[i] = ret_data[i];
        }
    }

    int mark_deleted(const hnswlib::labeltype id)
    {
        if (!index_inited)
        {
            std::runtime_error("Index not inited");
        }
        appr_alg->markDelete(id);
        return 0;
    }

    size_t knn_query(const data_t *query_vector, const size_t k, hnswlib::labeltype *ids, data_t *distance, const hnswlib::labeltype *allowed_ids, const size_t allowed_id_length, const hnswlib::labeltype *disallowed_ids, const size_t disallowed_id_length)
    {
        if (!index_inited)
        {
            std::runtime_error("Index not inited");
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
        if (res.size() < k)
        {
            // TODO: This is ok and we should return < K results, but for maintining compatibility with the old API we throw an error for now
            std::runtime_error("Not enough results");
        }
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
            std::runtime_error("Index not inited");
        }
        return appr_alg->ef_;
    }

    void set_ef(const size_t ef)
    {
        if (!index_inited)
        {
            std::runtime_error("Index not inited");
        }
        appr_alg->ef_ = ef;
    }
};

extern "C"
{
    Index<float> *create_index(const char *space_name, const int dim)
    {
        return new Index<float>(space_name, dim);
    }

    void init_index(Index<float> *index, const size_t max_elements, const size_t M, const size_t ef_construction, const size_t random_seed, const bool allow_replace_deleted, const bool is_persistent_index, const char *persistence_location)
    {
        index->init_index(max_elements, M, ef_construction, random_seed, allow_replace_deleted, is_persistent_index, persistence_location);
    }

    void load_index(Index<float> *index, const char *path_to_index, const bool allow_replace_deleted, const bool is_persistent_index)
    {
        index->load_index(path_to_index, allow_replace_deleted, is_persistent_index);
    }

    void persist_dirty(Index<float> *index)
    {
        index->persist_dirty();
    }

    void add_item(Index<float> *index, const float *data, const hnswlib::labeltype id, const bool replace_deleted)
    {
        index->add_item(data, id);
    }

    void get_item(Index<float> *index, const hnswlib::labeltype id, float *data)
    {
        index->get_item(id, data);
    }

    int mark_deleted(Index<float> *index, const hnswlib::labeltype id)
    {
        return index->mark_deleted(id);
    }

    size_t knn_query(Index<float> *index, const float *query_vector, const size_t k, hnswlib::labeltype *ids, float *distance, const hnswlib::labeltype *allowed_ids, const size_t allowed_id_length, const hnswlib::labeltype *disallowed_ids, const size_t disallowed_id_length)
    {
        return index->knn_query(query_vector, k, ids, distance, allowed_ids, allowed_id_length, disallowed_ids, disallowed_id_length);
    }

    int get_ef(Index<float> *index)
    {
        return index->appr_alg->ef_;
    }

    void set_ef(Index<float> *index, const size_t ef)
    {
        index->set_ef(ef);
    }

    int len(Index<float> *index)
    {
        return index->appr_alg->getCurrentElementCount() - index->appr_alg->getDeletedCount();
    }
}
