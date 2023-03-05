#include <iostream>
#include <cmath>
#include <bits/stdc++.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
using namespace std;

//structure used for type of parameter - mapper function
typedef struct function_parametres1
{
    vector<pair<long, string>> document_list;
    vector<vector<long>> *mappedd_v;
    bool *ended;
    int *noactive;
    pthread_mutex_t *newmutex;
    pthread_mutex_t *oldmutex;

    int *current_doc;
    int id;
    int mpow;
} func_map_par;
//structure used for type of parameter - reducer function
typedef struct function_parametres2
{
    vector<vector<long>>*mappedd_v;
    bool *ended;
    int id;
} func_reduce_par;

bool findSQ(int order, long number);

//mapper function
void *mapf(void *arg) {
    func_map_par par = *(func_map_par *)arg;
    //read documents while there are unopened ones
    while(true) {
        pthread_mutex_lock(par.newmutex);
        if(*(par.current_doc) >= (int)par.document_list.size()) {
            pthread_mutex_unlock(par.newmutex);
            break;
        }

        ifstream file(par.document_list[*par.current_doc].second, ios::in);
        //set current document index to the next one, the current one is marked as readed
        *(par.current_doc) += 1;
        pthread_mutex_unlock(par.newmutex);

        vector<vector<long>> lista(par.mpow);
        int no_elements;
        file >> no_elements;
        //for each number in ducument, verify if it is the perfect power of some other number
        //with findSQ function. If it is a perfect power, the element is added to a global
        //vector that contains all other perfect power numbers with same power
        for (int i = 0; i < no_elements; i++)
        {
            long nr;
            file >> nr;
            for (int i = 2; i < par.mpow + 2; i++)
            {
                if (findSQ(i, nr) && nr != 0)
                    par.mappedd_v->at(i - 2).push_back(nr);
            }
        }
        file.close();
    }
        pthread_mutex_lock(par.oldmutex);
        *(par.noactive)-= 1;
        if (*(par.noactive) == 0)
            *(par.ended) = true;
        pthread_mutex_unlock(par.oldmutex);
    pthread_exit(NULL);
}
//reducer fuction
void *reducef(void *arg)
{
    func_reduce_par par = *(func_reduce_par *)arg;
    while (*par.ended == false) {
        int i = 0;
        i++;
    }
    //count the unique elements of the same vector
    sort((*par.mappedd_v)[par.id].begin(), (*par.mappedd_v)[par.id].end());
    int count = unique((*par.mappedd_v)[par.id].begin(), (*par.mappedd_v)[par.id].end()) - (*par.mappedd_v)[par.id].begin();

    //write the counter in a document
    stringstream na;
    na << "out" << par.id + 2 << ".txt";
    ofstream file(na.str());
    file << count;
    file.close();
    pthread_exit(NULL);
}
//find if number is a perfect power with the power order
bool findSQ(int order, long number)
{
    double start = 0, end = number;
    while (true)
    {
        double mid = (start + end) / 2;
        double sum = 1;
        for (int i = 0; i < order; i++)
            sum = sum * mid;
        double err = abs(number - sum);
        if (err <= 0.01)
        {
            int sumi = 1;
            int imid = (int)(mid + 0.1);
            for (int i = 0; i < order; i++)
                sumi = sumi * imid;
            if (sumi == number)
                return true;
            return false;
        }
        if (sum > number)
            end = mid;
        else
            start = mid;
    }
}

int GetFileSize(string filename)
{
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

int main(int argc, char *argv[])
{
    int r, i, currentdoc = 0;
    bool ended = false;
    vector<pair<long, string>> document_list;
    int m_threadsN = atoi(argv[1]);
    int e_threadsN = atoi(argv[2]);
    int active = m_threadsN;
    ifstream docfile(argv[3], ios::in);
    int no_files;
    docfile >> no_files;
    for (int i = 0; i < no_files; i++)
    {
        string doc_name;
        docfile >> doc_name;
        std::ifstream in(doc_name, ifstream::ate | ifstream::binary);
        pair<long, string> p;
        p.second = doc_name;
        p.first = GetFileSize(doc_name);
        document_list.push_back(p);
    }
    sort(document_list.begin(), document_list.end());
    // VECTOR PT FIECARE DOCUMENT CU FIECARE LISTA CU NR DE PUTERI CU NR
    vector<vector<long>> mappedd_v(e_threadsN);

    pthread_mutex_t newmutex;
    pthread_mutex_init(&newmutex, NULL);
    pthread_mutex_t oldmutex;
    pthread_mutex_init(&oldmutex, NULL);

    func_map_par argm[m_threadsN];


    func_reduce_par argr[e_threadsN];

    //start threads
    pthread_t threads[m_threadsN + e_threadsN];
    void *status;
    for (i = 0; i < m_threadsN; i++)
    {
        //parametres for mapper fuction
        argm[i].document_list = document_list;
        argm[i].mappedd_v = &mappedd_v;
        argm[i].mpow = e_threadsN;
        argm[i].newmutex = &newmutex;
        argm[i].oldmutex = &oldmutex;
        argm[i].noactive = &active;
        argm[i].ended = &ended;
        argm[i].current_doc = &currentdoc;
        argm[i].id = i;
        r = pthread_create(&threads[i], NULL, mapf, &(argm[i]));
    }

    for (i = m_threadsN; i < e_threadsN + m_threadsN ; i++)
    {
        //parametres for reducer function
        argr[i - m_threadsN].mappedd_v = &mappedd_v;
        argr[i - m_threadsN].ended = &ended;
        argr[i - m_threadsN].id = i - m_threadsN;
        r = pthread_create(&threads[i], NULL, reducef, &(argr[i - m_threadsN]));
    }

    for (i = 0; i < m_threadsN + e_threadsN ; i++)
    {
        
        r = pthread_join(threads[i], &status);
    }

    pthread_mutex_destroy(&newmutex);
    pthread_mutex_destroy(&oldmutex);

    return 0;
}