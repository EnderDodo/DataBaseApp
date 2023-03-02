using System.Collections.Concurrent;
using DataBaseMovieApp.Models;

namespace DataBaseMovieApp;

public class EntityCreator
{
    public void Run(ConcurrentDictionary<string, List<string>> movieIdImdbTitlesNames,
        ConcurrentDictionary<string, Movie> movieIdImdbMovie,
        ConcurrentDictionary<string, Dictionary<string, List<string>>> movieIdImdbCategoryPersonsIdImdbs,
        ConcurrentDictionary<string, Person> personIdImdbPerson,
        int personId,
        ConcurrentDictionary<string, List<string>> movieIdImdbTagsNames,
        ConcurrentDictionary<string, Tag> tagNameTag,
        ConcurrentDictionary<string, float> movieIdImdbRating,
        ConcurrentDictionary<string, List<string>> movieIdTopIds,
        ConcurrentDictionary<string, HashSet<string>> directorIdImdbMoviesIdImdbs,
        ConcurrentDictionary<string, HashSet<string>> actorIdImdbMoviesIdImdbs)
    {
        //initializing movies

        Parallel.ForEach(movieIdImdbTitlesNames.Keys, new ParallelOptions()
        {
            MaxDegreeOfParallelism = -1
        }, filmId =>
        {
            var movie = movieIdImdbMovie[filmId];

            if (movieIdImdbCategoryPersonsIdImdbs.ContainsKey(filmId))
            {
                movie.Persons = new HashSet<Person>();
                foreach (var categoryPersonsId in movieIdImdbCategoryPersonsIdImdbs[filmId])
                {
                    foreach (var pId in movieIdImdbCategoryPersonsIdImdbs[filmId][categoryPersonsId.Key])
                    {
                        Person? person;
                        lock (movie)
                        {
                            if (!personIdImdbPerson.ContainsKey(pId))
                            {
                                person = new Person() { Name = pId };
                                person.Id = personId;
                                personId++;
                                personIdImdbPerson.TryAdd(pId, person);
                            }
                        }

                        lock (movie)
                        {
                            person = personIdImdbPerson[pId];
                            person.Category = categoryPersonsId.Key;
                            movie.Persons.Add(personIdImdbPerson[pId]);
                        }
                    }
                }
            }

            if (movieIdImdbTagsNames.ContainsKey(filmId))
            {
                movie.Tags = new HashSet<Tag>();
                foreach (var tagName in movieIdImdbTagsNames[filmId])
                {
                    movie.Tags.Add(tagNameTag[tagName]);
                }
            }

            if (movieIdImdbRating.ContainsKey(filmId))
                movie.Rating = movieIdImdbRating[filmId];

            if (movieIdTopIds.ContainsKey(filmId))
            {
                movie.Top = new List<Movie>();
                foreach (var topFilmId in movieIdTopIds[filmId])
                {
                    movie.Top.Add(movieIdImdbMovie[topFilmId]);
                }
            }
        });
        
        //initializing tags
        Parallel.ForEach(movieIdImdbTagsNames.Keys, filmId =>
        {
            var movie = movieIdImdbMovie[filmId];

            movie.Tags = new HashSet<Tag>();

            foreach (var tagName in movieIdImdbTagsNames[filmId])
            {
                movie.Tags.Add(tagNameTag[tagName]);
            }
        });

        //initializing persons
        Parallel.ForEach(movieIdImdbCategoryPersonsIdImdbs.Keys, new ParallelOptions() { MaxDegreeOfParallelism = -1 },
            filmId =>
            {
                foreach (var keyPersonCategory in movieIdImdbCategoryPersonsIdImdbs[filmId].Keys)
                {
                    foreach (var pId in movieIdImdbCategoryPersonsIdImdbs[filmId][keyPersonCategory])
                    {
                        Person? person;
                        lock (filmId)
                        {
                            if (!personIdImdbPerson.ContainsKey(pId))
                            {
                                person = new Person() { Name = pId };
                                person.Id = personId;
                                personId++;
                                personIdImdbPerson.TryAdd(pId, person);
                            }
                        }

                        lock (filmId)
                        {
                            person = personIdImdbPerson[pId];
                        }

                        if (keyPersonCategory == "director")
                        {
                            lock (person)
                            {
                                person.Category = "director";
                            }

                            if (person.Movies != null)
                            {
                                foreach (var tFilmId in directorIdImdbMoviesIdImdbs[pId])
                                {
                                    lock (person)
                                    {
                                        person.Movies.Add(movieIdImdbMovie[tFilmId]);
                                    }
                                }
                            }
                            else
                            {
                                lock (person)
                                {
                                    person.Movies = new HashSet<Movie>();
                                }

                                foreach (var tFilmId in directorIdImdbMoviesIdImdbs[pId])
                                {
                                    lock (person)
                                    {
                                        person.Movies.Add(movieIdImdbMovie[tFilmId]);
                                    }
                                }
                            }
                        }
                        else if (keyPersonCategory == "actor")
                        {
                            lock (person)
                            {
                                person.Category = "actor";
                            }

                            if (person.Movies != null)
                            {
                                foreach (var tFilmId in actorIdImdbMoviesIdImdbs[pId])
                                {
                                    lock (person)
                                    {
                                        person.Movies.Add(movieIdImdbMovie[tFilmId]);
                                    }
                                }
                            }
                            else
                            {
                                lock (person)
                                {
                                    person.Movies = new HashSet<Movie>();
                                }

                                foreach (var tFilmId in actorIdImdbMoviesIdImdbs[pId])
                                {
                                    lock (person)
                                    {
                                        person.Movies.Add(movieIdImdbMovie[tFilmId]);
                                    }
                                }
                            }
                        }
                    }
                }
            });


    }
}