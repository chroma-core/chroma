#include <iostream>
#include <fstream>
#include <queue>
#include <chrono>
#include "../../hnswlib/hnswlib.h"

#include <unordered_set>

using namespace std;
using namespace hnswlib;

/*
template <typename T>
void writeBinaryPOD(ostream& out, const T& podRef) {
    out.write((char*)&podRef, sizeof(T));
}

template <typename T>
static void readBinaryPOD(istream& in, T& podRef) {
    in.read((char*)&podRef, sizeof(T));
}*/
class StopW
{
    std::chrono::steady_clock::time_point time_begin;

public:
    StopW()
    {
        time_begin = std::chrono::steady_clock::now();
    }

    float getElapsedTimeMicro()
    {
        std::chrono::steady_clock::time_point time_end = std::chrono::steady_clock::now();
        return (std::chrono::duration_cast<std::chrono::microseconds>(time_end - time_begin).count());
    }

    void reset()
    {
        time_begin = std::chrono::steady_clock::now();
    }
};

void get_gt(
    float *mass,
    float *massQ,
    size_t vecsize,
    size_t qsize,
    L2Space &l2space,
    size_t vecdim,
    vector<std::priority_queue<std::pair<float, labeltype>>> &answers,
    size_t k)
{
    BruteforceSearch<float> bs(&l2space, vecsize);
    for (int i = 0; i < vecsize; i++)
    {
        bs.addPoint((void *)(mass + vecdim * i), (size_t)i);
    }
    (vector<std::priority_queue<std::pair<float, labeltype>>>(qsize)).swap(answers);
    // answers.swap(vector<std::priority_queue< std::pair< float, labeltype >>>(qsize));
    for (int i = 0; i < qsize; i++)
    {
        std::priority_queue<std::pair<float, labeltype>> gt = bs.searchKnn(massQ + vecdim * i, 10);
        answers[i] = gt;
    }
}

void get_gt(
    unsigned int *massQA,
    float *massQ,
    float *mass,
    size_t vecsize,
    size_t qsize,
    L2Space &l2space,
    size_t vecdim,
    vector<std::priority_queue<std::pair<float, labeltype>>> &answers,
    size_t k)
{
    // answers.swap(vector<std::priority_queue< std::pair< float, labeltype >>>(qsize));
    (vector<std::priority_queue<std::pair<float, labeltype>>>(qsize)).swap(answers);
    DISTFUNC<float> fstdistfunc_ = l2space.get_dist_func();
    cout << qsize << "\n";
    for (int i = 0; i < qsize; i++)
    {
        for (int j = 0; j < k; j++)
        {
            float other = fstdistfunc_(massQ + i * vecdim, mass + massQA[100 * i + j] * vecdim,
                                       l2space.get_dist_func_param());
            answers[i].emplace(other, massQA[100 * i + j]);
        }
    }
}

float test_approx(
    float *massQ,
    size_t vecsize,
    size_t qsize,
    HierarchicalNSW<float> &appr_alg,
    size_t vecdim,
    vector<std::priority_queue<std::pair<float, labeltype>>> &answers,
    size_t k)
{
    size_t correct = 0;
    size_t total = 0;
    // #pragma omp parallel for
    for (int i = 0; i < qsize; i++)
    {
        std::priority_queue<std::pair<float, labeltype>> result = appr_alg.searchKnn(massQ + vecdim * i, 10);
        std::priority_queue<std::pair<float, labeltype>> gt(answers[i]);
        unordered_set<labeltype> g;
        total += gt.size();
        while (gt.size())
        {
            g.insert(gt.top().second);
            gt.pop();
        }
        while (result.size())
        {
            if (g.find(result.top().second) != g.end())
                correct++;
            result.pop();
        }
    }
    return 1.0f * correct / total;
}

void test_vs_recall(
    float *massQ,
    size_t vecsize,
    size_t qsize,
    HierarchicalNSW<float> &appr_alg,
    size_t vecdim,
    vector<std::priority_queue<std::pair<float, labeltype>>> &answers,
    size_t k)
{
    // vector<size_t> efs = { 1,2,3,4,6,8,12,16,24,32,64,128,256,320 };//  = ; { 23 };
    vector<size_t> efs;
    for (int i = 10; i < 30; i++)
    {
        efs.push_back(i);
    }
    for (int i = 100; i < 2000; i += 100)
    {
        efs.push_back(i);
    }
    /*for (int i = 300; i <600; i += 20) {
        efs.push_back(i);
    }*/
    for (size_t ef : efs)
    {
        appr_alg.setEf(ef);
        StopW stopw = StopW();

        float recall = test_approx(massQ, vecsize, qsize, appr_alg, vecdim, answers, k);
        float time_us_per_query = stopw.getElapsedTimeMicro() / qsize;
        cout << ef << "\t" << recall << "\t" << time_us_per_query << " us\n";
        if (recall > 1.0)
        {
            cout << recall << "\t" << time_us_per_query << " us\n";
            break;
        }
    }
}
// void get_knn_quality(unsigned int *massA,size_t vecsize, size_t maxn, HierarchicalNSW<float> &appr_alg) {
//     size_t total = 0;
//     size_t correct = 0;
//     for (int i = 0; i < vecsize; i++) {
//         int *data = (int *)(appr_alg.linkList0_ + i * appr_alg.size_links_per_element0_);
//         //cout << "numconn:" << *data<<"\n";
//         tableint *datal = (tableint *)(data + 1);
//         total += maxn;
//         for (int j = 0; j < *data; j++) {
//             labeltype conn = appr_alg.getExternalLabel(datal[j]);
//             for (int k = 1; k <= maxn; k++) {
//                 if (massA[i * 100 + k] == conn) {
//                     correct++;
//                     break;
//                 }
//             }
//         }
//         if (i % 1000 == 0) {
//             cout << i << "\t" << correct << "\t" << total << "\n";
//             correct = 0;
//             total = 0;
//         }
//     }
// }
// #include "windows.h"

void sift_test()
{
    size_t vecsize = 980000;
    size_t qsize = 20000;
    // size_t qsize = 1000;
    // size_t vecdim = 4;
    size_t vecdim = 128;

    float *mass = new float[vecsize * vecdim];
    ifstream input("../../sift100k.bin", ios::binary);
    // ifstream input("../../1M_d=4.bin", ios::binary);
    input.read((char *)mass, vecsize * vecdim * sizeof(float));
    input.close();

    float *massQ = new float[qsize * vecdim];
    // ifstream inputQ("../siftQ100k.bin", ios::binary);
    ifstream inputQ("../../siftQ100k.bin", ios::binary);
    // ifstream inputQ("../../1M_d=4q.bin", ios::binary);
    inputQ.read((char *)massQ, qsize * vecdim * sizeof(float));
    inputQ.close();

    unsigned int *massQA = new unsigned int[qsize * 100];
    // ifstream inputQA("../knnQA100k.bin", ios::binary);
    ifstream inputQA("../../knnQA100k.bin", ios::binary);
    // ifstream inputQA("../../1M_d=4qa.bin", ios::binary);
    inputQA.read((char *)massQA, qsize * 100 * sizeof(int));
    inputQA.close();

    int maxn = 16;
    /*unsigned int *massA = new unsigned int[vecsize * 100];
    ifstream inputA("..\\..\\knngraph100k.bin", ios::binary);
    inputA.read((char *)massA, vecsize * 100 * sizeof(int));
    inputA.close();*/

    L2Space l2space(vecdim);
    // BruteforceSearch <float>bs(&l2space, vecsize);
    // for(int tr=1;tr<9;tr++)
// #define LOAD_I
#ifdef LOAD_I

    HierarchicalNSW<float> appr_alg(&l2space, "hnswlib_sift", false);
    // HierarchicalNSW<float> appr_alg(&l2space, "D:/stuff/hnsw_lib/nmslib/similarity_search/release/temp",true);
    // HierarchicalNSW<float> appr_alg(&l2space, "/mnt/d/stuff/hnsw_lib/nmslib/similarity_search/release/temp", true);

    // appr_alg_saved.saveIndex("d:\\hnsw-index.bin");
    // appr_alg_saved.loadIndex("d:\\hnsw-index2.bin", &l2space);
#else
    // return;
    // for (int u = 0; u < 10; u++) {
    /* PROCESS_MEMORY_COUNTERS pmc;

     GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
     SIZE_T virtualMemUsedByMe = pmc.WorkingSetSize;

     cout << virtualMemUsedByMe/1000/1000 << "\n";*/
    // HierarchicalNSW<float> appr_alg(&l2space, vecsize, 6, 40);
    HierarchicalNSW<float> appr_alg(&l2space, vecsize, 16, 200);

    cout << "Building index\n";
    StopW stopwb = StopW();
    for (int i = 0; i < 1; i++)
    {
        appr_alg.addPoint((void *)(mass + vecdim * i), (size_t)i);
    }
#pragma omp parallel for
    for (int i = 1; i < vecsize; i++)
    {
        appr_alg.addPoint((void *)(mass + vecdim * i), (size_t)i);
    }
    /*GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
    virtualMemUsedByMe = pmc.WorkingSetSize;
    cout << virtualMemUsedByMe / 1000 / 1000 << "\n";*/
    cout << "Index built, time=" << stopwb.getElapsedTimeMicro() * 1e-6 << "\n";
    // appr_alg.saveIndex("hnswlib_sift");

    // appr_alg.saveIndex("d:\\hnsw-index2.bin");

#endif

    // get_knn_quality(massA, vecsize, maxn, appr_alg);
    // return;

    vector<std::priority_queue<std::pair<float, labeltype>>> answers;
    size_t k = 10;
    cout << "Loading gt\n";
    // get_gt(mass, massQ, vecsize, qsize, l2space, vecdim, answers,k);
    get_gt(massQA, massQ, mass, vecsize, qsize, l2space, vecdim, answers, k);
    cout << "Loaded gt\n";
    for (int i = 0; i < 1; i++)
        test_vs_recall(massQ, vecsize, qsize, appr_alg, vecdim, answers, k);
    // cout << "opt:\n";
    // appr_alg.opt = true;

    return;
    // test_approx(mass, massQ, vecsize, qsize, appr_alg, vecdim, answers);
    //    //return;
    //
    //    cout << appr_alg.maxlevel_ << "\n";
    //    //CHECK:
    //    //for (size_t io = 0; io < vecsize; io++) {
    //    //    if (appr_alg.getExternalLabel(io) != io)
    //    //        throw new exception("bad!");
    //    //}
    //    DISTFUNC<float> fstdistfunc_ = l2space.get_dist_func();
    ////#pragma omp parallel for
    //    for (int i = 0; i < vecsize; i++) {
    //        int *data = (int *)(appr_alg.linkList0_ + i * appr_alg.size_links_per_element0_);
    //        //cout << "numconn:" << *data<<"\n";
    //        tableint *datal = (tableint *)(data + 1);
    //
    //        std::priority_queue< std::pair< float, tableint >> rez;
    //        unordered_set <tableint> g;
    //        for (int j = 0; j < *data; j++) {
    //            g.insert(datal[j]);
    //        }
    //        appr_alg.setEf(400);
    //        std::priority_queue< std::pair< float, tableint >> closest_elements = appr_alg.searchKnnInternal(appr_alg.getDataByInternalId(i), 17);
    //        while (closest_elements.size() > 0) {
    //            if (closest_elements.top().second != i) {
    //                 g.insert(closest_elements.top().second);
    //            }
    //            closest_elements.pop();
    //        }
    //
    //        for (tableint l : g) {
    //            float other = fstdistfunc_(appr_alg.getDataByInternalId(l), appr_alg.getDataByInternalId(i), l2space.get_dist_func_param());
    //            rez.emplace(other, l);
    //        }
    //        while (rez.size() > 32)
    //            rez.pop();
    //        int len = rez.size();
    //        *data = len;
    //        // check there are no loop connections created
    //        for (int j = 0; j < len; j++) {
    //            datal[j] = rez.top().second;
    //            if (datal[j] == i)
    //                throw new exception();
    //            rez.pop();
    //        }
    //
    //    }
    //
    //    //get_knn_quality(massA, vecsize, maxn, appr_alg);
    //    test_vs_recall( massQ, vecsize, qsize, appr_alg, vecdim, answers, k);
    //    /*test_vs_recall( massQ, vecsize, qsize, appr_alg, vecdim, answers, k);
    //    test_vs_recall( massQ, vecsize, qsize, appr_alg, vecdim, answers, k);
    //    test_vs_recall( massQ, vecsize, qsize, appr_alg, vecdim, answers, k);*/
    //
    //
    //
    //
    //
    //    /*for(int i=0;i<1000;i++)
    //        cout << mass[i] << "\n";*/
    //        //("11", std::ios::binary);
}
