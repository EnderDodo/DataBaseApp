using System.Collections.Concurrent;
using DataBaseMovieApp;
using DataBaseMovieApp.Models;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;

namespace DataBaseMovieApp;

public static class Program
{
    private static void ReinitializeDatabase()
    {
        var globalStopWatch = new Stopwatch();
        globalStopWatch.Start();
        
        var stopwatch = new Stopwatch();
        stopwatch.Start();

        var parser = new Parser();
        Console.WriteLine("Started parsing...");
        parser.Run();
        
        stopwatch.Stop();
        TimeSpan ts = stopwatch.Elapsed;
        string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
            ts.Hours, ts.Minutes, ts.Seconds,
            ts.Milliseconds / 10);
        Console.WriteLine($"Finished parsing... It took {elapsedTime}");
        
        stopwatch.Restart();
        
        var creator = new EntityCreator();
        Console.WriteLine("Started creating entities...");
        creator.Run(parser.MovieIdImdbTitlesNames, parser.MovieIdImdbMovie, parser.MovieIdImdbCategoryPersonsIdImdbs, 
            parser.PersonIdImdbPerson, parser.PersonId, parser.MovieIdImdbTagsNames, parser.TagNameTag, 
            parser.MovieIdImdbRating, parser.MovieIdTopIds, parser.DirectorIdImdbMoviesIdImdbs, parser.ActorIdImdbMoviesIdImdbs);
        
        ts = stopwatch.Elapsed;
        elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
            ts.Hours, ts.Minutes, ts.Seconds,
            ts.Milliseconds / 10);

        Console.WriteLine($"Finished creating entities... It took {elapsedTime}");

        using (var context = new DatabaseContext())
        {
            context.Database.EnsureDeleted();
            context.Database.EnsureCreated();
        }

        Console.WriteLine("Adding to database...");
        var connectionString = new NpgsqlConnectionStringBuilder()
        {
            Host = "localhost",
            Port = 5432,
            Database = "FilmAppDB",
            Username = "test",
            Password = "310803",
            Pooling = false,
            Timeout = 300,
            CommandTimeout = 300
        }.ToString();

        using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
        {
            connection.Open();

            using (var importer = connection.BeginBinaryImport("copy \"Movies\" from STDIN (FORMAT BINARY)"))
            {
                foreach (var movie in parser.MovieIdImdbMovie.Values)
                {
                    importer.StartRow();
                    importer.Write(movie.Id, NpgsqlDbType.Integer);
                    importer.Write(movie.IdImdb, NpgsqlDbType.Text);
                    importer.Write(movie.Rating, NpgsqlDbType.Real);
                    importer.Write(movie.OriginalTitle, NpgsqlDbType.Text);
                }
                importer.Complete();
            }

            using (var importer = connection.BeginBinaryImport("copy \"Persons\" from STDIN (FORMAT BINARY)"))
            {
                foreach (var person in parser.PersonIdImdbPerson.Values)
                {
                    importer.StartRow();
                    importer.Write(person.Id, NpgsqlDbType.Integer);
                    importer.Write(person.IdImdb, NpgsqlDbType.Text);
                    importer.Write(person.Name, NpgsqlDbType.Text);
                    importer.Write(person.Category, NpgsqlDbType.Text);
                }
                importer.Complete();
            }

            using (var importer = connection.BeginBinaryImport("copy \"Tags\" from STDIN (FORMAT BINARY)"))
            {
                foreach (var tag in parser.TagNameTag.Values)
                {
                    importer.StartRow();
                    importer.Write(tag.Id, NpgsqlDbType.Integer);
                    importer.Write(tag.Name, NpgsqlDbType.Text);
                }
                importer.Complete();
            }

            using (var importer = connection.BeginBinaryImport("copy \"Titles\" from STDIN (FORMAT BINARY)"))
            {
                foreach (var title in parser.Titles)
                {
                    importer.StartRow();
                    importer.Write(title.Id, NpgsqlDbType.Integer);
                    importer.Write(title.Name, NpgsqlDbType.Text);
                    importer.Write(title.MovieId, NpgsqlDbType.Integer);
                }

                importer.Complete();
            }

            using (var importer = connection.BeginBinaryImport("copy \"MovieMovie\" from STDIN (FORMAT BINARY)"))
            {
                foreach (var movie in parser.MovieIdImdbMovie.Values)
                {
                    if (movie.Top == null)
                        continue;

                    foreach (var topMovie in movie.Top)
                    {
                        importer.StartRow();
                        importer.Write(movie.Id, NpgsqlDbType.Integer);
                        importer.Write(topMovie.Id, NpgsqlDbType.Integer);
                    }
                }

                importer.Complete();
            }

            using (var importer = connection.BeginBinaryImport("copy \"MoviePerson\" from STDIN (FORMAT BINARY)"))
            {
                foreach (var movie in parser.MovieIdImdbMovie.Values)
                {
                    if (movie.Persons == null || movie.Persons.Count == 0)
                        continue;

                    var persons = movie.Persons;
                    foreach (var person in persons)
                    {
                        importer.StartRow();
                        importer.Write(movie.Id, NpgsqlDbType.Integer);
                        importer.Write(person.Id, NpgsqlDbType.Integer);
                    }
                }

                importer.Complete();
            }

            using (var importer = connection.BeginBinaryImport("copy \"MovieTag\" from STDIN (FORMAT BINARY)"))
            {
                foreach (var movie in parser.MovieIdImdbMovie.Values)
                {
                    if (movie.Tags == null)
                        continue;

                    foreach (var tag in movie.Tags)
                    {
                        importer.StartRow();
                        importer.Write(movie.Id, NpgsqlDbType.Integer);
                        importer.Write(tag.Id, NpgsqlDbType.Integer);
                    }
                }

                importer.Complete();
            }
        }

        globalStopWatch.Stop();
        
        TimeSpan gTs = globalStopWatch.Elapsed;
        string globalElapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
            gTs.Hours, gTs.Minutes, gTs.Seconds,
            gTs.Milliseconds / 10);

        Console.WriteLine("Execution time of ReinitDB: ");
        Console.WriteLine(globalElapsedTime);
    }

    static void Main()
    {
        Run();
    }

    [SuppressMessage("ReSharper.DPA", "DPA0000: DPA issues")]
    private static void Run()
    {
        using var context = new DatabaseContext();
        while (true)
        {
            Console.WriteLine("movie, person, tag");
            string? line = Console.ReadLine();

            if (line == null)
                continue;

            IQueryable<Movie> movies;

            switch (line.ToLower())
            {
                case "movie":

                    line = Console.ReadLine();

                    var movies1 = GetMovies(line);
                    
                    foreach (var movie in movies1)
                    {
                        Console.WriteLine(movie);
                    }

                    break;
                case "person":

                    line = Console.ReadLine();

                    if (string.IsNullOrEmpty(line))
                        break;

                    movies = context.Movies
                        .Where(movie => movie.Persons.Any(p => p.Name.ToLower() == line.ToLower()))
                        .Include(movie => movie.Persons)
                        .Include(movie => movie.Tags)
                        .Include(movie => movie.Top)!
                        .ThenInclude(movie => movie.Tags)
                        .Include(movie => movie.Top)!
                        .ThenInclude(movie => movie.Persons)
                        .AsSplitQuery();
                    
                    movies.ToList().ForEach(Console.WriteLine);

                    break;
                case "tag":

                    line = Console.ReadLine();

                    if (string.IsNullOrEmpty(line))
                        break;

                    movies = context.Movies
                        .Where(movie => movie.Tags.Any(t => t.Name.ToLower() == line.ToLower()))
                        .Include(movie => movie.Persons)
                        .Include(movie => movie.Tags)
                        .Include(movie => movie.Top)
                        .ThenInclude(movie => movie.Tags)
                        .Include(movie => movie.Top)
                        .ThenInclude(movie => movie.Persons)
                        .AsSplitQuery();


                    movies.ToList().ForEach(Console.WriteLine);

                    break;
                case "reinit":
                    Console.WriteLine("Are you sure?");
                    var ans = Console.ReadKey();
                    if (ans.Key == ConsoleKey.Y)
                    {
                        Console.WriteLine();
                        ReinitializeDatabase();
                    }

                    break;
            }

            Console.WriteLine();
        }
    }

    public static List<Movie> GetMovies(string? line)
    {
        var movies = new List<Movie>();
        using (var context = new DatabaseContext())
        {
            if (string.IsNullOrEmpty(line))
                return null;

            Stopwatch stopwatch = new();
            stopwatch.Start();

            var titles = context.Titles
                .Include(t => t.Movie)
                .Where(t => t.Name.ToLower() == line.ToLower())
                .GroupBy(t => t.MovieId).Select(x => x.First());

            if (!titles.Any())
            {
                return null;
            }

            foreach (var title in titles)
            {
                var movieId = title.MovieId;

                using (var cntxt = new DatabaseContext())
                {
                    var movieEntity = cntxt.Movies
                        .Where(movie => movieId == movie.Id)
                        .Include(x => x.Top)!
                        .ThenInclude(x => x.Tags)
                        .Include(x => x.Top)!
                        .ThenInclude(x => x.Persons)
                        .Include(x => x.Tags)
                        .Include(x => x.Persons)
                        .AsSplitQuery()
                        .Single();

                    movies.Add(movieEntity);
                }
            }
        }
        return movies;
    }
}