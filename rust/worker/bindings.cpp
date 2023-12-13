// Assumes that chroma-hnswlib is checked out at the same level as chroma
#include "../../../chroma_hnswlib/hnswlib/hnswlib.h"

// TODO: redo this / clean it up
// TODO: use const pointers where possible
// TODO: don't use data_t, use float for now since we don't support templating on the rust side

template <typename dist_t, typename data_t = float>
class Index
{
public:
    std::string space_name;
    int dim;
    size_t seed;
    size_t ef_search;

    bool normalize;
    bool index_inited;

    hnswlib::HierarchicalNSW<dist_t> *appr_alg;
    hnswlib::SpaceInterface<float> *l2space;

    Index(const std::string &space_name, const int dim) : space_name(space_name), dim(dim)
    {
        if (space_name == "l2")
        {
            l2space = new hnswlib::L2Space(dim);
        }
        if (space_name == "ip")
        {
            l2space = new hnswlib::InnerProductSpace(dim);
        }
        if (space_name == "cosine")
        {
            l2space = new hnswlib::InnerProductSpace(dim);
            normalize = true;
        }
        appr_alg = NULL;
        index_inited = false;
        ef_search = 10;
    }

    ~Index()
    {
        delete l2space;
        if (appr_alg)
        {
            delete appr_alg;
        }
    }

    void init_new_index(size_t max_elements, size_t M, size_t ef_construction, size_t random_seed, bool allow_replace_deleted, bool is_persistent_index, const std::string &persistence_location)
    {
        // if (index_inited) {
        //     // TODO: ERROR HANDLE!
        // }
        std::cout << "init_new_index" << std::endl;
        appr_alg = new hnswlib::HierarchicalNSW<dist_t>(l2space, max_elements, M, ef_construction, random_seed, allow_replace_deleted, normalize, is_persistent_index, persistence_location);
        appr_alg->ef_ = ef_search;
        index_inited = true;
    }

    void load_index(const std::string &path_to_index, bool allow_replace_deleted, bool is_persistent_index)
    {
        // check if index is inited and error if it is
        // check if path is valid
        // load index
        // return success
        // Use 0 for the max_elements since hnswlib will read it from the file and we don't want to override it
        appr_alg = new hnswlib::HierarchicalNSW<dist_t>(l2space, path_to_index, false, 0, allow_replace_deleted, normalize, is_persistent_index);
        index_inited = true;
    }

    void persist_dirty()
    {
        // check if index is inited
        // persist dirty
        appr_alg->persistDirty();
    }

    void add_item(data_t *data, hnswlib::labeltype id, bool replace_deleted = false)
    {
        // if replace_deleted, check if index allows it
        // check if index is inited
        // check if there is room for new item
        // check if id is already in use
        // check if id is deleted (maybe not necessary)
        // check if data is the right size
        // check if data is normalized (if needed) (maybe not necessary)
        // add item
        appr_alg->addPoint(data, id);
    }

    void get_item(hnswlib::labeltype id, data_t *data)
    {
        // check if index is inited
        // check if id is in use (hnswlib will throw an error if not, we should catch it and return a more useful error)
        // get item
        std::vector<data_t> ret_data = appr_alg->template getDataByLabel<data_t>(id); // This checks if id is deleted
        for (int i = 0; i < dim; i++)
        {
            data[i] = ret_data[i];
        }
    }

    int mark_deleted(hnswlib::labeltype id)
    {
        // check if index is inited
        // check if id is in use (hnswlib will throw an error if not, we should catch it and return a more useful error)
        // check if id is deleted (maybe not necessary)
        // mark deleted
        appr_alg->markDelete(id);
        return 0;
    }

    // I need to look into how JNA handles both cases but this is cleaner for now
    // For compatbility with java we narrow ids to ints here, we plan to replace this with strings in the future
    // Note that this means we are bound by the size of an int in Java in the iterim (plenty big enough for our purposes)
    void knn_query(data_t *query_vector, size_t k, int *ids, data_t *distance)
    {
        // check if index is inited
        // check if query_vector is the right size (should this happen here?)
        // check if query_vector is normalized (if needed) (maybe not necessary)
        // check if k is valid
        // normalize if needed
        // call knn_query
        // copy results into return arrays
        // return results
        std::priority_queue<std::pair<dist_t, hnswlib::labeltype>> res = appr_alg->searchKnn(query_vector, k);
        // copy results into return arrays
        // check if we have enough results
        if (res.size() < k)
        {
            // Handle this case, maybe its ok to just return what we have unlike python. For now return as many as we have
            // ask yury
            // we should null signify when we don't have enough results
        }
        int total_results = std::min(res.size(), k);
        for (int i = total_results - 1; i >= 0; i--)
        {
            std::pair<dist_t, hnswlib::labeltype> res_i = res.top();
            ids[i] = res_i.second;
            distance[i] = res_i.first;
            res.pop();
        }
    }

    int get_ef()
    {
        if (index_inited)
        {
            return appr_alg->ef_;
        }
        // TODO: ERROR IF NOT INITED
    }

    void set_ef(int ef)
    {
        ef_search = ef;
        if (index_inited)
        {
            appr_alg->ef_ = ef_search;
        }
        // TODO: ERROR IF NOT INITED
    }
};

extern "C"
{
    Index<float> *create_index(const char *space_name, const int dim)
    {
        std::cout << "create_index in c++" << std::endl;
        std::cout << "c++: space_name: " << space_name << std::endl;
        std::cout << "c++: dim: " << dim << std::endl;
        return new Index<float>(space_name, dim);
    }

    void init_index(Index<float> *index, size_t max_elements, size_t M, size_t ef_construction, size_t random_seed, bool allow_replace_deleted, bool is_persistent_index, const char *persistence_location)
    {
        std::cout << "c++: Path to inited index: " << persistence_location << std::endl;
        index->init_new_index(max_elements, M, ef_construction, random_seed, allow_replace_deleted, is_persistent_index, persistence_location);
    }

    void load_index(Index<float> *index, const char *path_to_index, bool allow_replace_deleted, bool is_persistent_index)
    {
        std::cout << "load_index in c++ from: " << path_to_index << std::endl;
        index->load_index(path_to_index, allow_replace_deleted, is_persistent_index);
    }

    void persist_dirty(Index<float> *index)
    {
        index->persist_dirty();
    }

    void add_item(Index<float> *index, float *data, hnswlib::labeltype id, bool replace_deleted)
    {
        index->add_item(data, id);
    }

    void get_item(Index<float> *index, hnswlib::labeltype id, float *data)
    {
        index->get_item(id, data);
    }

    // int mark_deleted(Index<float> *index, int id)
    // {
    //     return index->mark_deleted(id);
    // }

    void knn_query(Index<float> *index, float *query_vector, size_t k, int *ids, float *distance)
    {
        index->knn_query(query_vector, k, ids, distance);
    }

    int get_ef(Index<float> *index)
    {
        return index->appr_alg->ef_;
    }

    void set_ef(Index<float> *index, int ef)
    {
        index->set_ef(ef);
    }
}
