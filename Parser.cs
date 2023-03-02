using System.Collections.Concurrent;
using System.Globalization;
using DataBaseMovieApp.Models;

namespace DataBaseMovieApp;

public class Parser
{
    public ConcurrentDictionary<string, Dictionary<string, List<string>>> MovieIdImdbCategoryPersonsIdImdbs = new();
    private ConcurrentDictionary<string, List<string>> _movieIdImdbPersonsIdImdb = new(); 

    public ConcurrentDictionary<string, float> MovieIdImdbRating = new();
    public ConcurrentDictionary<string, List<string>> MovieIdImdbTagsNames = new();

    private ConcurrentDictionary<string, string> _movieTitleMovieIdImdb = new();

    public ConcurrentDictionary<string, List<string>> MovieIdImdbTitlesNames = new();

    public ConcurrentDictionary<string, HashSet<string>> DirectorIdImdbMoviesIdImdbs = new();
    public ConcurrentDictionary<string, HashSet<string>> ActorIdImdbMoviesIdImdbs = new();
    private ConcurrentDictionary<string, HashSet<string>> _tagNameMoviesIdImdbs = new();

    public ConcurrentDictionary<string, Movie> MovieIdImdbMovie = new();
    public ConcurrentDictionary<string, Tag> TagNameTag = new();
    public ConcurrentDictionary<string, Person> PersonIdImdbPerson = new();
    public List<Title> Titles = new();
    
    public ConcurrentDictionary<string, List<string>> MovieIdTopIds = new();

    public int PersonId = 1;

    public void Run()
    {
        
        const string movieCodesPath = @"C://Users/Denis/Documents/ml-latest/MovieCodes_IMDB.tsv";
        var movieIdTask = Task.Run(() =>
        {
            int movieId = 1;
            int titleId = 1;

            using var stream = new FileStream(movieCodesPath, FileMode.Open, FileAccess.Read, 
                FileShare.None, 64 * 1024,
                FileOptions.SequentialScan);
            using (var reader = new StreamReader(stream))
            {
                reader.ReadLine();
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineSpan = line.AsSpan();

                    var index = lineSpan.IndexOf('\t');
                    var movieIdImdb = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var movieTitle = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var region = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var lang = lineSpan.Slice(0, index).ToString();

                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var o = lineSpan.Slice(0, index).ToString();

                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);

                    var isOriginalTitleString = lineSpan.ToString();

                    var isOriginalTitle = isOriginalTitleString == "1";

                    if (lang == "en" || lang == "ru" || region == "US" || region == "RU" || region == "GB")
                    {
                        if (!MovieIdImdbMovie.ContainsKey(movieIdImdb))
                        {
                            var movie = new Movie();
                            movie.Id = movieId;
                            movieId++;
                            movie.Titles = new List<Title>();
                            movie.OriginalTitle = movieTitle;

                            MovieIdImdbMovie.TryAdd(movieIdImdb, movie);
                        }

                        if (MovieIdImdbTitlesNames.ContainsKey(movieIdImdb))
                        {
                            MovieIdImdbTitlesNames[movieIdImdb].Add(movieTitle);
                        }
                        else
                        {
                            MovieIdImdbTitlesNames[movieIdImdb] = new List<string> { movieTitle };
                        }

                        var title = new Title() { Id = titleId, Name = movieTitle, MovieId = MovieIdImdbMovie[movieIdImdb].Id };
                        titleId++;
                        Titles.Add(title);
                        MovieIdImdbMovie[movieIdImdb].Titles.Add(title);

                        if (!_movieTitleMovieIdImdb.ContainsKey(movieTitle))
                        {
                            _movieTitleMovieIdImdb.TryAdd(movieTitle, movieIdImdb);
                        }

                        if (isOriginalTitle)
                        {
                            MovieIdImdbMovie[movieIdImdb].OriginalTitle = movieTitle;
                        }
                    }
                }
            }
        });
        
        const string pathActorsDirectorsNames = @"C:\Users\Denis\Documents\ml-latest\ActorsDirectorsNames_IMDB.txt";
        const string pathActorsDirectorsCodes = @"C://Users/Denis/Documents/ml-latest/ActorsDirectorsCodes_IMDB.tsv";

        var actorsTask = Task.Run(() =>
        {
            using (var stream = new FileStream(pathActorsDirectorsNames, FileMode.Open, 
                       FileAccess.Read, FileShare.None,
                       64 * 1024, FileOptions.SequentialScan))
            using (var reader = new StreamReader(stream))
            {
                reader.ReadLine();
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineSpan = line.AsSpan();

                    var index = lineSpan.IndexOf('\t');
                    var personIdImdb = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var personName = lineSpan.Slice(0, index).ToString();

                    var person = new Person() { Name = personName };
                    person.Id = PersonId;
                    lock (person)
                    {
                        PersonId++;    
                    }
                    PersonIdImdbPerson[personIdImdb] = person;
                }
            }

            movieIdTask.Wait();

            using (var stream = new FileStream(pathActorsDirectorsCodes, FileMode.Open, 
                       FileAccess.Read, FileShare.None,
                       64 * 1024, FileOptions.SequentialScan))
            using (var reader = new StreamReader(stream))
            {
                reader.ReadLine();
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineSpan = line.AsSpan();

                    int index;
                    index = lineSpan.IndexOf('\t');
                    var movieIdImdb = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    if (!MovieIdImdbTitlesNames.ContainsKey(movieIdImdb))
                        continue;

                    index = lineSpan.IndexOf('\t');
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var personId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var category = lineSpan.Slice(0, index).ToString();

                    if (!(category == "actor" || category == "actress" || category == "self" || category == "director"))
                        continue;

                    if (category == "actress" || category == "self")
                        category = "actor";

                    if (MovieIdImdbCategoryPersonsIdImdbs.ContainsKey(movieIdImdb))
                    {
                        if (MovieIdImdbCategoryPersonsIdImdbs[movieIdImdb].ContainsKey(category))
                        {
                            MovieIdImdbCategoryPersonsIdImdbs[movieIdImdb][category].Add(personId);
                        }
                        else
                        {
                            MovieIdImdbCategoryPersonsIdImdbs[movieIdImdb][category] = new List<string>() { personId };
                        }

                        _movieIdImdbPersonsIdImdb[movieIdImdb].Add(personId);
                    }
                    else
                    {
                        MovieIdImdbCategoryPersonsIdImdbs[movieIdImdb] = new Dictionary<string, List<string>>();
                        MovieIdImdbCategoryPersonsIdImdbs[movieIdImdb][category] = new List<string>() { personId };

                        _movieIdImdbPersonsIdImdb.TryAdd(movieIdImdb, new List<string>() { personId });
                    }
                }
            }
        });

        const string pathRating = @"C://Users/Denis/Documents/ml-latest/Ratings_IMDB.tsv";
        var ratingTask = Task.Run(() =>
        {
            using (var stream = new FileStream(pathRating, FileMode.Open, FileAccess.Read, 
                       FileShare.None, 64 * 1024,
                       FileOptions.SequentialScan))
            using (var reader = new StreamReader(stream))
            {
                reader.ReadLine();
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineSpan = line.AsSpan();

                    var index = lineSpan.IndexOf('\t');
                    var movieIdImdb = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf('\t');
                    var rating = lineSpan.Slice(0, index).ToString();

                    if (!movieIdTask.IsCompleted)
                    {
                        movieIdTask.Wait();
                    }

                    if (!MovieIdImdbTitlesNames.ContainsKey(movieIdImdb))
                        continue;

                    MovieIdImdbRating[movieIdImdb] = float.Parse(rating, CultureInfo.InvariantCulture.NumberFormat);
                    ;
                }
            }
        });

        const string pathLinks = @"C:\Users\Denis\Documents\ml-latest\links_IMDB_MovieLens.csv";
        var linksIdTask = Task.Run(() =>
        {

            var idImdbId = new Dictionary<string, string>();
            using (var stream = new FileStream(pathLinks, FileMode.Open, FileAccess.Read, 
                       FileShare.None, 64 * 1024,
                       FileOptions.SequentialScan))
            using (var reader = new StreamReader(stream))
            {
                reader.ReadLine();
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineSpan = line.AsSpan();

                    var index = lineSpan.IndexOf(',');
                    var movieLensId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf(',');
                    var movieIdImdb = lineSpan.Slice(0, index).ToString();

                    idImdbId[movieLensId] = string.Concat("tt", movieIdImdb);
                }
            }

            return idImdbId;
        });
        
        const string pathTagCodes = @"C:\Users\Denis\Documents\ml-latest\TagCodes_MovieLens.csv";
        var codeTagTask = Task.Run(() =>
        {

            var id = 1;

            var codeTagTagName = new Dictionary<string, string>();
            using (var stream = new FileStream(pathTagCodes, FileMode.Open, FileAccess.Read, 
                       FileShare.None, 64 * 1024,
                       FileOptions.SequentialScan))
            using (var reader = new StreamReader(stream))
            {
                reader.ReadLine();
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineSpan = line.AsSpan();

                    int index;
                    index = lineSpan.IndexOf(',');
                    var codeTag = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    var tagName = lineSpan.ToString();

                    codeTagTagName[codeTag] = tagName;
                    TagNameTag[tagName] = new Tag() { Name = tagName, Id = id };
                    id++;
                }
            }

            return codeTagTagName;
        });
        
        const string pathTagScores = @"C:\Users\Denis\Documents\ml-latest\TagScores_MovieLens.csv";
        var tagsTask = Task.Run(() =>
        {
            var idImdbId = linksIdTask.Result;
            var codeTagTagName = codeTagTask.Result;


            using (var stream = new FileStream(pathTagScores, FileMode.Open, FileAccess.Read, 
                       FileShare.None, 64 * 1024,
                       FileOptions.SequentialScan))
            using (var reader = new StreamReader(stream))
            {
                reader.ReadLine();
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    var lineSpan = line.AsSpan();

                    var index = lineSpan.IndexOf(',');
                    var movieId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    index = lineSpan.IndexOf(',');
                    var tagId = lineSpan.Slice(0, index).ToString();
                    lineSpan = lineSpan.Slice(index + 1);

                    var relevance = lineSpan.ToString();

                    if (!movieIdTask.IsCompleted)
                    {
                        movieIdTask.Wait();
                    }

                    var filmId = idImdbId[movieId];

                    if (!MovieIdImdbTitlesNames.ContainsKey(filmId) ||
                        !(float.Parse(relevance, CultureInfo.InvariantCulture.NumberFormat) > 0.5f))
                        continue;

                    if (MovieIdImdbTagsNames.ContainsKey(filmId))
                    {
                        MovieIdImdbTagsNames[filmId].Add(codeTagTagName[tagId]);
                    }
                    else
                    {
                        MovieIdImdbTagsNames[filmId] = new List<string>() { codeTagTagName[tagId] };
                    }
                }
            }
        });

        Task.WaitAll(actorsTask, ratingTask, tagsTask, codeTagTask, movieIdTask, linksIdTask);

        //personId -> movieIds

        var personMoviesTask = Task.Run(() =>
        {
            foreach (var movieId in MovieIdImdbCategoryPersonsIdImdbs.Keys)
            {
                foreach (var category in MovieIdImdbCategoryPersonsIdImdbs[movieId].Keys)
                {
                    foreach (var personId in MovieIdImdbCategoryPersonsIdImdbs[movieId][category])
                    {
                        if (category == "actor")
                        {
                            if (ActorIdImdbMoviesIdImdbs.ContainsKey(personId))
                            {
                                ActorIdImdbMoviesIdImdbs[personId].Add(movieId);
                            }
                            else
                            {
                                ActorIdImdbMoviesIdImdbs.TryAdd(personId, new HashSet<string>() { movieId });
                            }
                        }

                        if (category == "director")
                        {
                            if (DirectorIdImdbMoviesIdImdbs.ContainsKey(personId))
                            {
                                DirectorIdImdbMoviesIdImdbs[personId].Add(movieId);
                            }
                            else
                            {
                                DirectorIdImdbMoviesIdImdbs.TryAdd(personId, new HashSet<string>() { movieId });
                            }
                        }
                    }
                }
            }
        });

        //movieId -> tags, tag -> movieIds

        var tagMovieIdsTask = Task.Run(() =>
        {
            foreach (var movieId in MovieIdImdbTitlesNames.Keys)
            {
                if (!MovieIdImdbTagsNames.ContainsKey(movieId))
                    continue;

                foreach (var tag in MovieIdImdbTagsNames[movieId])
                {
                    if (_tagNameMoviesIdImdbs.ContainsKey(tag))
                    {
                        _tagNameMoviesIdImdbs[tag].Add(movieId);
                    }
                    else
                    {
                        _tagNameMoviesIdImdbs.TryAdd(tag, new HashSet<string>() { movieId });
                    }
                }
            }
        });

        Task.WaitAll(personMoviesTask, tagMovieIdsTask);

        #region top10
        var count = 0;

        Console.WriteLine("initializing top10");


        Parallel.ForEach(MovieIdImdbTitlesNames.Keys, new ParallelOptions() { MaxDegreeOfParallelism = -1 }, movieIdImdb =>
        {
            ConcurrentDictionary<float, HashSet<string>> affinityMoviesIdImdbs = new();
            var addedMoviesIdImdbs = new HashSet<string>();

            List<string> persons = null;
            List<string> tags = null;

            if (_movieIdImdbPersonsIdImdb.ContainsKey(movieIdImdb))
                persons = _movieIdImdbPersonsIdImdb[movieIdImdb];
            if (MovieIdImdbTagsNames.ContainsKey(movieIdImdb))
                tags = MovieIdImdbTagsNames[movieIdImdb];
            
            if (tags != null)
            {
                foreach (var tag in tags)
                {
                    if (_tagNameMoviesIdImdbs.ContainsKey(tag))
                    {
                        foreach (var othermovieIdImdb in _tagNameMoviesIdImdbs[tag])
                        {
                            if (addedMoviesIdImdbs.Contains(othermovieIdImdb) || othermovieIdImdb == movieIdImdb)
                                continue;

                            var affinity = GetAffinity(movieIdImdb, othermovieIdImdb);

                            if (affinityMoviesIdImdbs.ContainsKey(affinity))
                                affinityMoviesIdImdbs[affinity].Add(othermovieIdImdb);
                            else
                                affinityMoviesIdImdbs.TryAdd(affinity, new HashSet<string>() { othermovieIdImdb });

                            addedMoviesIdImdbs.Add(othermovieIdImdb);
                        }
                    }
                }
            }
            if (persons != null)
            {
                foreach (var personId in persons)
                {
                    if (ActorIdImdbMoviesIdImdbs.ContainsKey(personId))
                    {
                        foreach (var othermovieIdImdb in ActorIdImdbMoviesIdImdbs[personId])
                        {
                            if (addedMoviesIdImdbs.Contains(othermovieIdImdb) || othermovieIdImdb == movieIdImdb)
                                continue;

                            var affinity = GetAffinity(movieIdImdb, othermovieIdImdb);

                            if (affinityMoviesIdImdbs.ContainsKey(affinity))
                                affinityMoviesIdImdbs[affinity].Add(othermovieIdImdb);
                            else
                                affinityMoviesIdImdbs.TryAdd(affinity, new HashSet<string>() { othermovieIdImdb });

                            addedMoviesIdImdbs.Add(othermovieIdImdb);
                        }
                    }

                    if (DirectorIdImdbMoviesIdImdbs.ContainsKey(personId))
                    {
                        foreach (var othermovieIdImdb in DirectorIdImdbMoviesIdImdbs[personId])
                        {
                            if (addedMoviesIdImdbs.Contains(othermovieIdImdb) || othermovieIdImdb == movieIdImdb)
                                continue;

                            var affinity = GetAffinity(movieIdImdb, othermovieIdImdb);

                            if (affinityMoviesIdImdbs.ContainsKey(affinity))
                                affinityMoviesIdImdbs[affinity].Add(othermovieIdImdb);
                            else
                                affinityMoviesIdImdbs.TryAdd(affinity, new HashSet<string>() { othermovieIdImdb });

                            addedMoviesIdImdbs.Add(othermovieIdImdb);
                        }
                    }
                }
            }

            int k = 0;

            if (affinityMoviesIdImdbs.Keys.Count == 0)
                return;

            lock (MovieIdTopIds)
            {
                MovieIdTopIds.TryAdd(movieIdImdb, new List<string>());    
            }
            
            var orderedDict = affinityMoviesIdImdbs.OrderByDescending(x => x.Key);
            foreach (var affMovieIdImdb in orderedDict)
            {
                foreach (var movie in affMovieIdImdb.Value)
                {
                    lock (MovieIdTopIds)
                    {
                        MovieIdTopIds[movieIdImdb].Add(movie);    
                    }
                    k++;
                    if (k == 10)
                        break;
                }

                if (k == 10)
                    break;
            }

            count++;
            if (count % 100000 == 0)
            {
                Console.WriteLine(count);
            }
        });

        Console.WriteLine("Finished initializing top 10");
        #endregion
        
        
    }


    private float GetAffinity(string curMovieId, string otherMovieId)
    {
        float personsAffinity;
        float tagsAffinity;
        float rating;
        
        if (MovieIdImdbTagsNames.TryGetValue(curMovieId, out var curTags)
            && MovieIdImdbTagsNames.TryGetValue(otherMovieId, out var otherTags))
        {
            var intersectingTagsCount = curTags.Intersect(otherTags).Count();
            tagsAffinity = (float)intersectingTagsCount / (curTags.Count * 3);
        }
        else
        {
            tagsAffinity = 0f;
        }
        
        if (_movieIdImdbPersonsIdImdb.TryGetValue(curMovieId, out var curPersons)
            && _movieIdImdbPersonsIdImdb.TryGetValue(otherMovieId, out var otherPersons))
        {
            var intersectingPersonsCount = curPersons.Intersect(otherPersons).Count();
            personsAffinity = (float)intersectingPersonsCount / (curPersons.Count * 6);
        }
        else
        {
            personsAffinity = 0f;
        }

        if (tagsAffinity == 0f && personsAffinity == 0f)
            return -1f;

        if (!MovieIdImdbRating.TryGetValue(otherMovieId, out rating))
            rating = 0;

        return personsAffinity + tagsAffinity + rating / 20;
    }
}