/**
 *
 * @author: Hadas Jacobi, 2018.
 *
 * This client reads the file points_awarded.h - Eurovision data from 1975-2016.
 * It then finds the winner from each year, and counts how many wins each country had.
 *
 */

#include <cstdio>
#include <iostream>
#include <sstream>
#include <array>
#include <fstream>
#include <vector>
#include "MapReduceFramework.h"
#include "MapReduceFramework.cpp"

typedef std::pair<std::string, int> Points_received_by_country;
typedef std::vector<Points_received_by_country> Points_in_year;

// K1 is the year the Eurovision happened
class Kyear : public K1 {
public:
    Kyear(int year) : year(year) {}

    int year;

    virtual bool operator<(const K1 &other) const {
        return year < static_cast<const Kyear&>(other).year;
    }
};


// V1 is a vector of {country, points_received} for all countries that participated that year
class Vpointlist : public V1 {
public:
    Vpointlist(Points_in_year pointlist) : pointlist(pointlist) {}

    Points_in_year pointlist;
};


// K2 & K3 are the winners of each year
class Kwinner : public K2, public K3 {
public:
    Kwinner(std::string country) : country(country) {}

    std::string country;

    virtual bool operator<(const K2 &other) const {
        return country < static_cast<const Kwinner&>(other).country;
    }
    virtual bool operator<(const K3 &other) const {
        return country < static_cast<const Kwinner&>(other).country;
    }
};


// V3 is the total number of wins
class Vwins : public V3 {
public:
    Vwins(int wins) : wins(wins) {}

    int wins;
};



class eurovisionClient : public MapReduceClient {
public:
    /**
     * Finds the winning country for each year by choosing the country that received the most
     * points.
     */
    void map(const K1 *key, const V1 *value, void *context) const {
        Points_in_year pointlist = static_cast<const Vpointlist *>(value)->pointlist;
        Points_received_by_country winner = pointlist[0];
        for (auto country_points : pointlist){
            if (country_points.second > winner.second){
                winner = {country_points.first, country_points.second};
            }
        }

        int year = static_cast<const Kyear *>(key)->year;
//        std::cout << "Winner for " << year << ": " << winner.first << "\n";

        Kwinner* k2 = new Kwinner(winner.first);
        emit2(k2, nullptr, context);
    }

    /**
     * Counts how many wins in total a country had.
     */
    virtual void reduce(const IntermediateVec *pairs,
                        void *context) const {
        std::string country = static_cast<const Kwinner *>(pairs->at(0).first)->country;

        int wins = pairs->size();
        for (const IntermediatePair &pair: *pairs) {
            delete pair.first;
            delete pair.second;
        }
        Kwinner* k3 = new Kwinner(country);
        Vwins* v3 = new Vwins(wins);
        emit3(k3, v3, context);
    }

    virtual void printt(IntermediateVec & v) const {

        for (IntermediatePair& pair: v) {
            std::string country = ((const Kwinner*)pair.first)->country;
            int wins = ((const Vwins*)pair.second)->wins;
            printf("%s: %d win\n", country.c_str(), wins);

        }

    }


};

void process_data(std::string path, InputVec* inputVec){
    std::ifstream input_file;
    input_file.open(path);

    std::string line;
    if(input_file.is_open())
    {
        while(std::getline(input_file, line)) //get 1 row as a string
        {
            Points_in_year points_in_year(0);

            std::istringstream iss(line); // put line into stringstream
            int raw_year;
            iss >> raw_year;

            std::string country;
            int points;
            while(iss >> country >> points) //country by country
            {
                points_in_year.emplace_back(Points_received_by_country(country, points));
            }

            Kyear* year = new Kyear(raw_year);
            Vpointlist* pointlist = new Vpointlist(points_in_year);

            inputVec->emplace_back(std::pair<Kyear*, Vpointlist*>(year, pointlist));
        }
    }
}


int main(int argc, char **argv) {
    for (int i = 0; i < 1000; i++)
    {


        eurovisionClient client;
        InputVec inputVec;
        OutputVec outputVec;

        // TODO - enter your path to "new_clients/points_awarded.in"
        process_data("/cs/usr/dvir_jacobo/Desktop/ex3_try2/points_awarded.in", &inputVec);


        JobHandle job = startMapReduceJob(client, inputVec, outputVec, 2);
        waitForJob(job);

        printf("Total Eurovision wins 1975-2016:\n");
        for (OutputPair &pair: outputVec) {
            std::string country = ((const Kwinner *) pair.first)->country;
            int wins = ((const Vwins *) pair.second)->wins;
            printf("%s: %d win%s\n",
                   country.c_str(), wins, wins > 1 ? "s" : "");
            delete pair.first;
            delete pair.second;
        }


        for (auto j : inputVec) {
            delete j.first;
            delete j.second;
        }

        closeJobHandle(job);
    }

    return 0;
}

