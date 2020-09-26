//
// Created by dvir_jacobo on 5/13/19.
//

#include "MapReduceFramework.h"
#include "MapReduceClient.h"

#include <atomic>
#include "Barrier.h"
//#include "Barrier.cpp"
#include <algorithm>
#include <pthread.h>
#include <semaphore.h>
#include <iostream>

#define PERCENTAGE 100

using namespace std;


struct pthread_context{
    int id;
    size_t inpSize;
    size_t* sortedSize; // we added pointer because this veriable is not constant. so if thread is setting this variable
                        // we want it to be setted anywhere.
    int numOfThreads;
    const InputVec* inpVec;
    OutputVec* outVec;
    const MapReduceClient* client;
    std::atomic<unsigned int>* atomic_counter;
    std::atomic<unsigned int>* job_counter;
    Barrier* barrier;
    pthread_mutex_t* sortedMutex;
    pthread_mutex_t* shuffledMutex; // DONE! named reducedMut
    pthread_mutex_t* outputMutex; // DONE! named emit3Mut
    pthread_mutex_t* stageMutex; // DONE! named stageMut
    pthread_mutex_t* percentageMutex;
    pthread_mutex_t* doneShuufledMutex;
    sem_t* shuffledSem;
    JobState* curr_state;
    std::vector<IntermediateVec> * sortedVectors;
    std::vector<IntermediateVec> * shuffledVectors;
    int* doneShuffle;


    IntermediateVec intermediateVEctor; // Important! this field is not initialized in the startFunction so that each thread has it's own intermediateVEctor!
                                        // so each thread fills its own intermediateVector and the sortedVectors is fulled  with all the itermediate vectors from all
                                        // the threads.



};



struct this_job{
    int numOfThread;
    int flag;
    pthread_context** pthread_lst;
    pthread_t* list;
    JobState* curr_statee;
};




void waitHelper(this_job* job_context, int index);
K2* find_max_key(K2 *value, pthread_context *curr_pthread, unsigned long index);
void delete_threads(this_job* job_context, int index);
void* make_map_reduce(void *cont);
void thirdReducing(pthread_context *curr_thread);
void secondReducing(pthread_context* curr_thread);
void forthReducing(pthread_context* curr_thread);
void fifthReducing(pthread_context* curr_thread);








bool cmp(const IntermediatePair &first, const IntermediatePair &second);
K2* find_max(pthread_context *curr_thread);
void mapping(pthread_context* curr_thread);
void sorting(pthread_context *curr_thread);
void curr_barrier(pthread_context *curr_thread);
void shuffleing(pthread_context *curr_context);
void reducing(pthread_context *curr_thread);

void emit2(K2 *key, V2 *value, void *context)
{
    auto next_thread = (pthread_context*)context;
    IntermediatePair intermediatePair(key, value);
    next_thread->intermediateVEctor.push_back(intermediatePair);
}

void emit3(K3* key, V3 *value, void* context){
    auto t_context = (pthread_context*)context;
    pthread_mutex_lock(t_context->outputMutex);
    OutputPair outputPair(key, value);
    t_context->outVec->push_back(outputPair);
    pthread_mutex_unlock(t_context->outputMutex);

}



void* make_map_reduce(void *cont)
{

    auto curr_context = (pthread_context*)cont;


    mapping(curr_context);
    sorting(curr_context);


    pthread_mutex_lock(curr_context->sortedMutex);
    if (!curr_context->intermediateVEctor.empty()) // each thread pushs it's own intermediateVector to the common sortedVector.
    {

        curr_context->sortedVectors->push_back(curr_context->intermediateVEctor);
    }
    pthread_mutex_unlock(curr_context->sortedMutex);

    *curr_context->doneShuffle = 0;
    curr_barrier(curr_context);

//    *curr_context->doneShuffle = 0;


//    printf("id: %d\n", curr_context->id);

    if(curr_context->id == 0)
    { // Only the main thread doing shuffling
//        *curr_context->doneShuffle = 0;
        shuffleing(curr_context); // starts the shuffling after setting the percentage and the job counter to 0.
//        pthread_mutex_unlock(curr_context->doneShuufledMutex);
//        pthread_mutex_unlock(curr_context->doneShuufledMutex);


    }

//    printf("id: %d\n", curr_context->id);

//    printf("my id %d, my doneShuffle is %d\n", curr_context->id, *curr_context->doneShuffle);
    forthReducing(curr_context);


//    printf("done my job! thread num %d\n", curr_context->id);
    return cont;
}



void startMapReduceJob_Helper(int multiTreadLevel, pthread_t threads[], pthread_context* context_list[], int index)
{
    if (index >= multiTreadLevel){
        return;
    }
    pthread_create(threads + index, nullptr, make_map_reduce, *(context_list + index)); // removed the pointer in the last argument

    startMapReduceJob_Helper(multiTreadLevel, threads, context_list, ++index);

}



JobHandle startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiTreadLevel)
{

    auto sortedSize                 = new size_t(0);
    auto context_list               = new pthread_context*[multiTreadLevel];
    auto atomic_counter             = new std::atomic<unsigned  int>{0};
    auto job_counter                =  new std::atomic<unsigned int>{0};
    auto threads                    = new pthread_t[multiTreadLevel];


    auto barrier                    = new Barrier(multiTreadLevel);

    auto * sortedMutex = new pthread_mutex_t;
    auto * shuffledMutex = new pthread_mutex_t;
    auto * outputMutex = new pthread_mutex_t;
    auto * stageMutex = new pthread_mutex_t;
    auto * prctgMutex = new pthread_mutex_t;
    auto * doneShuffledMutex = new pthread_mutex_t;


    auto * shuffledSem  = new sem_t;
    auto sortedAllVectors           = new std::vector<IntermediateVec>;
    auto shuffleAllvectors          = new std::vector<IntermediateVec>;



    if(pthread_mutex_init(sortedMutex, nullptr))
    {
        exit(1);
    }

    if(pthread_mutex_init(shuffledMutex, nullptr))
    {
        exit(1);
    }
    if(pthread_mutex_init(outputMutex, nullptr))
    {
        exit(1);
    }
    if(pthread_mutex_init(prctgMutex, nullptr))
    {
        exit(1);
    }
    if(pthread_mutex_init(stageMutex, nullptr))
    {
        exit(1);
    }
    if (pthread_mutex_init(doneShuffledMutex, nullptr))
    {
        exit(1);
    }

    if(sem_init(shuffledSem,0 ,0))
    {
        exit(1);
    }
    //

    int doneShuffle;


    auto curr_state = new JobState;
    curr_state->stage = UNDEFINED_STAGE;
    curr_state->percentage = 0.0;

    for(int index = 0; index < multiTreadLevel; index++)
    {


        auto next = new pthread_context{index, inputVec.size(), sortedSize, multiTreadLevel, &inputVec, &outputVec,
                                &client, atomic_counter, job_counter, barrier, sortedMutex, shuffledMutex,
                                outputMutex, stageMutex, prctgMutex, doneShuffledMutex, shuffledSem, curr_state,
                                sortedAllVectors, shuffleAllvectors, &doneShuffle};

        context_list[index] = next;
    }

    startMapReduceJob_Helper(multiTreadLevel, threads, context_list, 0);



    auto* Job = new this_job{multiTreadLevel, 0, context_list, threads, curr_state};

    return Job;
}




void waitHelper(this_job* job_context, int index) {
    if (index >= job_context->numOfThread) {

        job_context->flag = 1;
        return;

    }
    if (pthread_join(job_context->list[index], nullptr))
    {
        exit(1);
    }
//    printf("end tesk thread num: %d\n", job_context->pthread_lst[index]->id);

    waitHelper(job_context, ++index);
}

//void waitForJob(JobHandle job)
//{
//    auto j_handle = (this_job*)job;
//    for (int i = 0; i < j_handle->numOfThread; ++i)
//    {
//        if(pthread_join(j_handle->list[i], nullptr))
//        {
//            exit(1);
//        }
//    }
//
//    j_handle->flag = 1;
//}

void waitForJob(JobHandle job)
{
    auto j_handle = (this_job*)job;
    waitHelper(j_handle, 0);

}



void getJobState(JobHandle job, JobState* state)
{
    auto j_handle = (this_job*)job;
    if (j_handle->pthread_lst[0]->curr_state->stage == UNDEFINED_STAGE)
    {
        state->stage = UNDEFINED_STAGE;
        state->percentage = 0.0;
        return;
    }

    if (j_handle->pthread_lst[0]->curr_state->stage == REDUCE_STAGE)
    {
        state->stage = REDUCE_STAGE;
        if ((*j_handle->pthread_lst[0]->job_counter / (float)*(j_handle->pthread_lst[0]->sortedSize)) * 100 > 100.0)
        {
            state->percentage = 100.0;
        }

        else
            {
            state->percentage = (*j_handle->pthread_lst[0]->job_counter / (float)*(j_handle->pthread_lst[0]->sortedSize)) * 100;
            }
        return;
    }

    if (j_handle->pthread_lst[0]->curr_state->stage == MAP_STAGE)
    {
        state->stage = MAP_STAGE;
        if ((*j_handle->pthread_lst[0]->atomic_counter / (float)j_handle->pthread_lst[0]->inpSize) * 100 > 100.0)
        {
            state->percentage = 100.0;
        }
        else
        {
            state->percentage = (*j_handle->pthread_lst[0]->atomic_counter / (float)j_handle->pthread_lst[0]->inpSize) * 100;
        }
        return;
    }
//    state->stage = j_handle->curr_statee->stage;
//    state->percentage = j_handle->curr_statee->percentage;

}



void closeJobHandle(JobHandle job)
{

    auto j_handle = (this_job*)job;
    if (j_handle->flag == 0)
    {
        waitForJob(job);
    }

    delete (j_handle->pthread_lst[0]->sortedSize);
    delete(j_handle->pthread_lst[0]->atomic_counter);
//    delete(j_handle->pthread_lst[0]->job_counter);
    delete(j_handle->pthread_lst[0]->barrier);

    pthread_mutex_destroy(j_handle->pthread_lst[0]->sortedMutex);
    delete(j_handle->pthread_lst[0]->sortedMutex);
    pthread_mutex_destroy(j_handle->pthread_lst[0]->shuffledMutex);
    delete(j_handle->pthread_lst[0]->shuffledMutex);
    pthread_mutex_destroy(j_handle->pthread_lst[0]->outputMutex);
    delete(j_handle->pthread_lst[0]->outputMutex);
    sem_destroy(j_handle->pthread_lst[0]->shuffledSem);
    delete(j_handle->pthread_lst[0]->shuffledSem);
    pthread_mutex_destroy(j_handle->pthread_lst[0]->percentageMutex);
    delete(j_handle->pthread_lst[0]->percentageMutex);
    pthread_mutex_destroy(j_handle->pthread_lst[0]->stageMutex);
    delete(j_handle->pthread_lst[0]->stageMutex);

    j_handle->pthread_lst[0]->sortedVectors->clear();
    delete(j_handle->pthread_lst[0]->sortedVectors);
//    j_handle->pthread_lst[0]->shuffledVectors->clear(); // no need we pop out in shuffling
    delete(j_handle->pthread_lst[0]->shuffledVectors);

    for (int i = 0; i < j_handle->numOfThread; i++)
    {
        delete j_handle->pthread_lst[i];
    }
    delete[] j_handle->pthread_lst;
    delete[] j_handle->list;
    delete(j_handle->curr_statee);
    delete(j_handle);
}









bool cmp(const IntermediatePair &first, const IntermediatePair &second){
    return *first.first < *second.first;
}


void emit2(K2 *key, V2 *value, void *context)
{
    auto next_thread = (pthread_context*)context;
    IntermediatePair intermediatePair(key, value);
    next_thread->intermediateVEctor.push_back(intermediatePair);
}


void mapping(pthread_context* curr_thread)
{
//    printf("dontShuffle state before starting mapping %d\n", *curr_thread->doneShuffle);
    pthread_mutex_lock(curr_thread->stageMutex);
    curr_thread->curr_state->stage = MAP_STAGE;
    pthread_mutex_unlock(curr_thread->stageMutex);

    pthread_mutex_lock(curr_thread->percentageMutex);
    unsigned int old_value = (*(curr_thread->atomic_counter))++; // each map operation increments the atomic counter.
    pthread_mutex_unlock(curr_thread->percentageMutex);
    while(old_value < (curr_thread->inpSize))
    {
        curr_thread->client->map(curr_thread->inpVec->at(old_value).first, curr_thread->inpVec->at(old_value).second, curr_thread);
        pthread_mutex_lock(curr_thread->percentageMutex);
        old_value = (*(curr_thread->atomic_counter))++;
        pthread_mutex_unlock(curr_thread->percentageMutex);
    }

//    printf("dontShuffle state after mapping %d\n", *curr_thread->doneShuffle);

//    pthread_mutex_lock(curr_thread->percentageMutex);
//    curr_thread->curr_state->percentage += 100.0 / curr_thread->numOfThreads;
//    pthread_mutex_unlock(curr_thread->percentageMutex);

}




void sorting(pthread_context *curr_thread)
{

    try {


        std::sort(curr_thread->intermediateVEctor.begin(),
                  curr_thread->intermediateVEctor.end(), cmp);
    }

    catch (std::bad_alloc &e)
    {
        return;
    }

}

void curr_barrier(pthread_context *curr_thread){
    curr_thread->barrier->barrier();

}


K2* findMaxKey(pthread_context* curr_thread)
{
    unsigned long tid = 0;
    while (curr_thread->sortedVectors->at(tid).empty() && tid <= curr_thread->sortedVectors->size())
    {
        tid++;
    }
    if (tid == curr_thread->sortedVectors->size())
    {
        exit(1);
    }

    K2 *tempKey = curr_thread->sortedVectors->at(tid).back().first;
    for (unsigned long j = tid; j < curr_thread->sortedVectors->size(); ++j)
    {
        if (curr_thread->sortedVectors->at(j).empty())
        {
            continue;
        }

        if (*tempKey < *curr_thread->sortedVectors->at(j).back().first)
        {
            tempKey = curr_thread->sortedVectors->at(j).back().first;
        }

    }
    return tempKey;
}




void shuffleing(pthread_context *curr_context)

{
//    printf("dontShuffle state before starting shuffleing %d\n", *curr_context->doneShuffle);
    pthread_mutex_lock(curr_context->stageMutex); // TODO changed added lock to zero percentage
    curr_context->curr_state->stage = REDUCE_STAGE; // when to main thread starts doing shuffling it can change the stafe to REDUCE because there is no stage for shuffling
    pthread_mutex_unlock(curr_context->stageMutex);
    for(auto &vec :*(curr_context->sortedVectors)){

//            printf("sortedSize %zu\n", *(curr_context->sortedSize));
        *(curr_context->sortedSize) += vec.size(); // we store the size of sortedAllVectors in order to calculate the percentage of reduce stage. in each time.
    }

//    while (sortedNotEmpty)
    size_t counter = *curr_context->sortedSize;
//    printf("counter = %zu\n", counter);
//    while(!curr_context->sortedVectors->empty())
//    printf("dontShuffle state before starting while shuffleing %d\n", *curr_context->doneShuffle);
    *curr_context->doneShuffle = 0;
    while(counter > 0) {
        auto k2 = findMaxKey(curr_context);
        IntermediateVec last;

//        printf("dontShuffle state before entering for shuffling %d\n", *curr_context->doneShuffle);
        for (auto &vec :*(curr_context->sortedVectors)) {
            if (vec.empty()) {
                continue;
            }
            while (!(vec.empty()) && !(*vec.back().first < *k2) && !(*k2 < *vec.back().first))
            {
//                printf("dontShuffle state before inner while shuffling %d\n", *curr_context->doneShuffle);
                last.push_back(vec.back());
//                pthread_mutex_lock(curr_context->sortMutex); // No need! only thread 0 is updates the sortedVectors after the barrier.
                vec.pop_back();
//                pthread_mutex_unlock(curr_context->sortMutex); // No need! only thread 0 is updates the sortedVectors after the barrier.
                counter--;
            }
        }

        pthread_mutex_lock(curr_context->shuffledMutex);
        curr_context->shuffledVectors->push_back(last);
        pthread_mutex_unlock(curr_context->shuffledMutex);
//        printf("thread num %d increnements semaphor semaphore val is: %d\n ", curr_context->id,
//               static_cast<int>(curr_context->shuffledVectors->size()));
        sem_post(curr_context->shuffledSem);
    }

    pthread_mutex_lock(curr_context->shuffledMutex);
    *curr_context->doneShuffle = 1; // we sets the dontShuffle before the clear operation because the last take time.
//    curr_context->sortedVectors->clear();
    pthread_mutex_unlock(curr_context->shuffledMutex);

}





void forthReducing(pthread_context* curr_thread)
{

    while (true)
    {

//        printf("doneShuffle for id %d : %d\n", curr_thread->id, *curr_thread->doneShuffle);
        pthread_mutex_lock(curr_thread->shuffledMutex);

        if (curr_thread->shuffledVectors->empty() && *curr_thread->doneShuffle == 1)
        {
//            printf("thead num %d, exits reducing!\n", curr_thread->id);
            pthread_mutex_unlock(curr_thread->shuffledMutex);
            break;
        }


        if (!curr_thread->shuffledVectors->empty())
        {
//            printf("thread num %d reduces!\n", curr_thread->id);
            IntermediateVec reduceVec = curr_thread->shuffledVectors->back();
            curr_thread->shuffledVectors->pop_back();

            *curr_thread->job_counter += reduceVec.size(); //TODO - to put * job_counter is a pointer;
            curr_thread->client->reduce(&reduceVec, curr_thread);
//            printf("thread num %d decrease semaphor, semaphore size is %d \n", curr_thread->id, static_cast<int>(curr_thread->shuffledVectors->size()));
            pthread_mutex_unlock(curr_thread->shuffledMutex);
            sem_wait(curr_thread->shuffledSem);
            continue;


        }


        if (curr_thread->shuffledVectors->empty())
            {
//            printf("empty! thread num: %d my doneShuffleState: %d\n", curr_thread->id, *curr_thread->doneShuffle);


                if (*curr_thread->doneShuffle)
            {
//                printf("thread num %d exits reducing2!\n", curr_thread->id);
                pthread_mutex_unlock(curr_thread->shuffledMutex);
                break;
            }

            if (!(*curr_thread->doneShuffle))
                {

//                printf("thread num %d decrease semaphor, semaphore size is %d: \n", curr_thread->id, static_cast<int>(curr_thread->shuffledVectors->size()));
                    pthread_mutex_unlock(curr_thread->shuffledMutex); // important to set the unlock before the wait!
                    sem_wait(curr_thread->shuffledSem);

                }

            }

    }

    pthread_mutex_lock(curr_thread->shuffledMutex);
    for (int i = 0; i < curr_thread->numOfThreads; ++i)
    {
        sem_post(curr_thread->shuffledSem);
    }
    pthread_mutex_unlock(curr_thread->shuffledMutex);
//    printf("i'm here! thread num: %d\n", curr_thread->id);


}



