#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include "Barrier.cpp"
#include "pthread.h"
#include <atomic>

#define SYSCALL_SUCCESS 0
#define COMPLETE_PERCENTAGE 100

using std::cerr;
using std::endl;
using std::get;
using std::cout;

/**
 * a vector of type std::vector<std::pair<K2*, V2*>>, the elements that were
 * yielded during the Map phase and were processed by this specific thread
 */
typedef std::vector<IntermediatePair> intermediateVec;

/**
 * A map containing all of this job's pthread index'es, along with
 * their corresponding intermediate vectors and mutex'es
 */
typedef std::map<int, std::pair<intermediateVec, pthread_mutex_t>> pthread_map;

/**
 * A struct that represent a Job's context and contains all of it's arguments
 */
typedef struct JobContext{

    /**
     * The number of threads allocated for this job
     */
    int numThreads;

    /**
     * The client which represents the task of this JobContext
     */
    const MapReduceClient* client;

    /**
     * a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
     */
    const InputVec* inputVec;

    /**
     * a vector of type std::vector<std::pair<K3*, V3*>>,
     * to which the output elements will be added before returning.
     */
    OutputVec* outputVec;

    /**
     * Barrier that allows passage to the reduce
     * phase when Map and Shuffle phases are done
     */
    Barrier* reducePhase_barrier;

//    /**
//     * Conditional variable that is signaled to when
//     * an InputPair is mapped and shuffle can initiate
//     */
//    pthread_cond_t shuffleCondition /*= PTHREAD_COND_INITIALIZER*/;

    /**
     * a map of type std::map< k2*, std::vector<v2*>>
     * that contains the combined Map phase output
     */
    IntermediateMap* intermediateMap /*= new IntermediateMap()*/;

    /**
     * a vector of type std::vector< k2*> to contain
     * the key-set of the Intermediate Map pairs.
     */
    std::vector<K2*>* intermediateKeySet /*= new std::vector<K2*>()*/;

    /**
     * Atomic counter that keeps track of the
     * number of Threads that are done running
     */
    std::atomic<unsigned int>* doneThreads /*= new std::atomic<unsigned int>(0)*/;

    /**
     * Atomic counter that keeps track of the MapPhase and ReducePhase progress
     * (different than the keys-processed counter below because it is incremented
     * prior to the actual processing of the key)
     */
    std::atomic<unsigned int>* inputCounter /*= new std::atomic<unsigned int>(0)*/;

    /**
     * Atomic counter that keep track of the current Job state
     */
    std::atomic<unsigned int>* currentStage /*= new std::atomic<unsigned int>(UNDEFINED_STAGE)*/;

    /**
     * Atomic counter that keep track of how many keys were
     * processed during the current stage (MapPhase and ReducePhase)
     */
    std::atomic<unsigned int>* keysProcessed /*= new std::atomic<unsigned int>(0)*/;

    /**
     * Atomic counter that keep track of how many keys were
     * processed during the current stage (ShufflePhase)
     */
    std::atomic<unsigned int>* shuffleProcessed /*= new std::atomic<unsigned int>(0)*/;

    /**
     * Atomic counter that keep track of the number
     * of Intermediate Pair available for Shuffling
     */
    std::atomic<unsigned int>* shuffleTotal /*= new std::atomic<unsigned int>(0)*/;

    /**
     * A map containing all of this job's threads index'ex and data
     */
    std::vector<pthread_t>* pthreads /*= nullptr*/;

    /**
     * A map containing all of this job's thread index'ex and data
     */
    pthread_map* jobThreads /*= nullptr*/;

    /**
     * mutex object for the JobState member variable
     */
    pthread_mutex_t state_mutex /*= PTHREAD_MUTEX_INITIALIZER*/;

    /**
     * mutex object for the OutputVec member variable
     */
    pthread_mutex_t output_mutex /*= PTHREAD_MUTEX_INITIALIZER*/;

    /**
     * Boolean member which indicates whether or not this Job is finished
     */
    bool isFinished /*= false*/;

} JobContext;

/**
 * A function that receives the Job of this pthread as an argument,
 * and returns a pair that contains the current running pthread's
 * intermediate vector and it's mutex. If not found, return nullptr.
 */
std::pair<intermediateVec, pthread_mutex_t>* pthread_current(JobContext* Job)
{
    for(int i = 0; i < Job->numThreads; i++)
    {
        if(pthread_equal(pthread_self() ,Job->pthreads->operator[](i)))
        {
            return &(Job->jobThreads->operator[](i));
        }
    }
    return nullptr; // if current pthread was not found in Job
}

/**
 * perform mutex_lock and assert that the return value is valid
 */
void safe_mutex_lock(pthread_mutex_t *_mutex)
{
    int retVal = pthread_mutex_lock(_mutex);

    if(retVal != SYSCALL_SUCCESS)
    {
        cerr << "system error: " << "pthread_mutex_lock" << endl;
        exit(EXIT_FAILURE);
    }
}

/**
 * perform mutex_unlock and assert that the return value is valid
 */
void safe_mutex_unlock(pthread_mutex_t *_mutex)
{
    int retVal = pthread_mutex_unlock(_mutex);

    if(retVal != SYSCALL_SUCCESS)
    {
        cerr << "system error: " << "pthread_mutex_unlock" << endl;
        exit(EXIT_FAILURE);
    }
}

/**
 * perform mutex_destroy and assert that the return value is valid
 */
void safe_mutex_destroy(pthread_mutex_t *_mutex)
{
    int retVal = pthread_mutex_destroy(_mutex);

    if(retVal != SYSCALL_SUCCESS)
    {
        cerr << "system error: " << "pthread_mutex_destroy" << endl;
        exit(EXIT_FAILURE);
    }
}

/**
 * This function shuffles all of the available mapped IntermediatePairs
 * @param context The context of the calling thread's Job
 */
void shufflePairs(void* context)
{
    auto Job = (JobContext *) context;

    for (int i = 0; i < Job->numThreads -1; i++)
    {
        intermediateVec& vec = Job->jobThreads->operator[](i).first;
        pthread_mutex_t* mutex = &Job->jobThreads->operator[](i).second;

        safe_mutex_lock(mutex);
        while (!vec.empty())
        {


            IntermediatePair& currentPair = vec.back();
            Job->intermediateMap->operator[](currentPair.first).
                    push_back(currentPair.second); // Insert pair to appropriate cell
            vec.pop_back();

            safe_mutex_lock(&(Job->state_mutex));
            (*(Job->shuffleProcessed))++; // Update shuffleTotal counter
            safe_mutex_unlock(&(Job->state_mutex));


        }
        safe_mutex_unlock(mutex);
    }
}

/**
 * This function calls the Reduce-Phase routine
 * @param context The context of the calling thread's Job
 */
void* ReducePhase(void* context)
{
    auto Job = (JobContext *) context;

    safe_mutex_lock(&Job->state_mutex); // Update Job's state to Reduce stage
    if(*Job->currentStage == SHUFFLE_STAGE)
    {
        *Job->currentStage = REDUCE_STAGE;
        *Job->keysProcessed = 0;
        *Job->inputCounter = 0; // Reset counters in order to use them in Reduce Phase
    }
    safe_mutex_unlock(&Job->state_mutex);

    unsigned int current_index; // The index of Shuffled pair handled by the current thread
    while(*Job->inputCounter < Job->intermediateKeySet->size()) // While there are more pairs to map
    {
        current_index =
                (*(Job->inputCounter))++; // Advance input counter so that other threads wont Reduce this index

        if(current_index < Job->intermediateKeySet->size())
        {
            Job->client->reduce(Job->intermediateKeySet->at(current_index), Job->intermediateMap->
                    at(Job->intermediateKeySet->at(current_index)), Job); // Place current Pair in OutputVec

            (*(Job->keysProcessed))++; // Increase KP counter by one
        }
    }
    (*(Job->doneThreads))++;
    pthread_exit(NULL);
}

/**
 * This function calls the Map-Phase routine for a single thread
 * @param context The context of the calling thread's Job
 */
void* MapPhase(void* context)
{
    auto Job = (JobContext *) context;

    safe_mutex_lock(&(Job->state_mutex)); // Update Job's state to Map stage
    if(*Job->currentStage == UNDEFINED_STAGE)
    {
        *Job->currentStage = MAP_STAGE;
    }
    safe_mutex_unlock(&(Job->state_mutex));

    unsigned int current_index; // The index of InputVec handled by the current thread
    while(Job->inputCounter->load() < Job->inputVec->size()
          && *Job->currentStage == MAP_STAGE) // While there are more pairs to map during MapPhase
    {
        current_index =
                (*(Job->inputCounter))++; // Advance input counter so that other threads wont Map this index

        if(current_index < Job->inputVec->size())
        {
            Job->client->map(Job->inputVec->at(current_index).first,
                             Job->inputVec->at(current_index).second, Job); // map the current InputPair

            (*(Job->keysProcessed))++; // Increase KP counter by one

            // safe_mutex_lock(&(Job->jobThreads->operator[](Job->numThreads - 1)).second);
            //pthread_cond_broadcast(&Job->shuffleCondition); // Signal Shuffle thread
            // safe_mutex_unlock(&(Job->jobThreads->operator[](Job->numThreads - 1)).second);
        }
    }

    Job->reducePhase_barrier->barrier(); // Wait for other threads prior to reduce phase

    return ReducePhase(Job);
}

/**
 * This function calls the Shuffle-Phase routine
 * @param context The context of the calling thread's Job
 */
void* ShufflePhase(void* context)
{
    auto Job = (JobContext *) context;

    while(*Job->keysProcessed < Job->inputVec->size()) // While Map Phase is in progress
    {
        // Wait for a Map thread signal
        //safe_mutex_lock(&(*(pthread_current(Job))).second);
        //pthread_cond_wait(&Job->shuffleCondition, &(*(pthread_current(Job))).second);
        //safe_mutex_unlock(&(*(pthread_current(Job))).second);
        shufflePairs(Job); // Shuffle available Pairs to the Intermediate Map
    }

    safe_mutex_lock(&Job->state_mutex); // Update Job's state to Shuffle stage
    if(*Job->currentStage == MAP_STAGE)
    {
        *Job->currentStage = SHUFFLE_STAGE;
    }
    safe_mutex_unlock(&Job->state_mutex);

    shufflePairs(Job); // Shuffle all of the remaining pairs

    for (auto & MapVec : *(Job->intermediateMap)) // Insert all map keys to key-set vector
    {
        Job->intermediateKeySet->push_back(MapVec.first);
    }

    Job->reducePhase_barrier->barrier(); // Indicate other threads Shuffle Phase has ended
    return ReducePhase(Job);
}

/**
 * This function produces a (K2*,V2*) pair and pushes
 * it into the current thread's intermediate vector
 */
void emit2 (K2* key, V2* value, void* context)
{
    auto Job = (JobContext *) context;

    safe_mutex_lock(&(*(pthread_current(Job))).second); // Block shuffle thread access

    (*(pthread_current(Job))).first.emplace_back(key,
                                                 value); // place IntermediatePair in the running thread's IntermediateVec
    (*Job->shuffleTotal)++; // Update number of available pair for shuffle thread

    safe_mutex_unlock(&(*pthread_current(Job)).second);
}

/**
 * This function produces a (K3*,V3*) pair
 */
void emit3 (K3* key, V3* value, void* context)
{
    auto Job = (JobContext *) context;

    safe_mutex_lock(&Job->output_mutex); // Block OutputVec access for other threads
    Job->outputVec->emplace_back(key, value); // place Pair in OutputVec
    safe_mutex_unlock(&Job->output_mutex);
}

/**
 * This function starts running the MapReduce algorithm
 * (with several threads) and returns a JobHandle
 * @return a JobHandler
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    auto Job = new JobContext {multiThreadLevel, &client, &inputVec, &outputVec, new Barrier(multiThreadLevel),
//                               PTHREAD_COND_INITIALIZER,
                               new IntermediateMap(), new std::vector<K2*>(), new std::atomic<unsigned int> (0),
                               new std::atomic<unsigned int> (0),new std::atomic<unsigned int> (0),
                               new std::atomic<unsigned int> (0),new std::atomic<unsigned int> (0),
                               new std::atomic<unsigned int> (0), nullptr, nullptr, PTHREAD_MUTEX_INITIALIZER,
                               PTHREAD_MUTEX_INITIALIZER, false}; // Create a corresponding JobContext struct object
    int retVal;
    auto pthreads = new std::vector<pthread_t>(multiThreadLevel); // pthread object repository
    auto jobThreads = new pthread_map(); // pthread context repository for this job
    Job->jobThreads = jobThreads;
    Job->pthreads = pthreads;

    for(int i = 0; i < multiThreadLevel - 1; i++)
    {
        jobThreads->insert(std::pair<int, std::pair<intermediateVec, pthread_mutex_t>>
                                   (i, std::pair<intermediateVec,pthread_mutex_t>(intermediateVec(),
                                                                                  PTHREAD_MUTEX_INITIALIZER)));

        retVal = pthread_create(&(*pthreads)[i], NULL, &MapPhase,
                                Job); // Create Map threads and Contexts

        if(retVal != SYSCALL_SUCCESS)
        {
            cerr << "system error: " << "pthread_create failed" << endl;
            exit(EXIT_FAILURE);
        }
    }
    jobThreads->insert(std::pair<int, std::pair<intermediateVec, pthread_mutex_t>>
                               (multiThreadLevel - 1, std::pair<intermediateVec,pthread_mutex_t>(intermediateVec(),
                                                                                                 PTHREAD_MUTEX_INITIALIZER)));

    retVal = pthread_create((&(*pthreads)[multiThreadLevel - 1]),
                            NULL, &ShufflePhase, Job); // Create the shuffle thread and Context

    if(retVal != SYSCALL_SUCCESS)
    {
        cerr << "system error: " << "pthread_create failed" << endl;
        exit(EXIT_FAILURE);
    }

    return Job;
}

/**
 *  a function gets the job handle returned by
 *  startMapReduceFramework and waits until it is finished.
 */
void waitForJob(JobHandle job)
{
    auto Job = (JobContext *) job;

    for (auto &Thread: *Job->pthreads) // Launch all of the Job's threads
    {
        int retVal = pthread_join(Thread, NULL);

        if(retVal != SYSCALL_SUCCESS)
        {
            cerr << "system error: " << "pthread_join failed" << endl;
            exit(EXIT_FAILURE);
        }
    }
    while ((int) *Job->doneThreads < Job->numThreads); // While not threads have finished running
    Job->isFinished = true; // Indication that the Job is finished
}

/**
 * this function gets a job handle and updates the
 * state of the job in to the given JobState struct
 */
void getJobState(JobHandle job, JobState* state)
{
    auto Job = (JobContext *) job;
    safe_mutex_lock(&(Job->state_mutex)); // Prevent stage change of the Job's state

    switch (Job->currentStage->load())
    {
        case UNDEFINED_STAGE:
            state->stage = UNDEFINED_STAGE;
            state->percentage = 0;
            break;

        case MAP_STAGE:
            state->stage = MAP_STAGE;
            state->percentage = ((float)(*Job->keysProcessed) / Job->inputVec->size()) * COMPLETE_PERCENTAGE;
            break;

        case SHUFFLE_STAGE:
            state->stage = SHUFFLE_STAGE;
            state->percentage = ((float)(*Job->shuffleProcessed) / *Job->shuffleTotal) * COMPLETE_PERCENTAGE;
            break;

        case REDUCE_STAGE:
            state->stage = REDUCE_STAGE;
            state->percentage = ((float)(*Job->keysProcessed) / Job->intermediateMap->size()) * COMPLETE_PERCENTAGE;
            break;
    }
    safe_mutex_unlock(&(Job->state_mutex));
}

/**
 * Releasing all resources of a job. Releasing resources before
 * the job is finished is prevented. After this function is called
 * the job handle will be invalid.
 */
void closeJobHandle(JobHandle job)
{
    auto Job = (JobContext *) job;

    if(!Job->isFinished) // Wait for Job to be finished before closing
    {
        waitForJob(Job);
    }
    delete Job->reducePhase_barrier; // delete barriers and destroy conditional variables

//    retVal = pthread_cond_destroy(&Job->shuffleCondition);
//    if(retVal != SYSCALL_SUCCESS)
//    {
//        cerr << "system error: " << "pthread_cond_destroy" << endl;
//        exit(EXIT_FAILURE);
//    }

    for(int i = 0; i < Job->numThreads; i++) // Destroy each thread's mutex
    {
        pthread_mutex_t* mutex = &Job->jobThreads->operator[](i).second;

        pthread_mutex_destroy(mutex);
    }

    delete Job->jobThreads; // Release data structure memory
    delete Job->intermediateMap;
    delete Job->pthreads;
    delete Job->intermediateKeySet;
    delete Job->inputCounter;
    delete Job->shuffleTotal;
    delete Job->shuffleProcessed;
    delete Job->currentStage;
    delete Job->keysProcessed;
    delete Job->doneThreads;

    safe_mutex_destroy(&Job->output_mutex); // destroy remaining mutex'es
    safe_mutex_destroy(&Job->state_mutex);

    delete Job; // Finally, release the primary job memory
}