using System.Text;

namespace DataBaseMovieApp.Models;

public class Movie
{
    public int Id { get; set; }
    public string IdImdb { get; set; }
    public virtual HashSet<Person>? Persons { get; set; }
    public virtual HashSet<Tag>? Tags { get; set; }
    public float Rating { get; set; }
    public virtual List<Movie>? Top { get; set; }
    public virtual List<Title> Titles { get; set; }

    public string OriginalTitle { get; set; }

    public float GetAffinity(Movie other)
    {
        float tagsAffinity;
        float personsAffinity;

        if (Tags != null && other.Tags != null && Tags.Count() > 0 && other.Tags.Count() > 0)
        {
            int intersectingTagsCount = Tags.Intersect(other.Tags).Count();
            tagsAffinity = (float)intersectingTagsCount * 2 / (Tags.Count * 6);
        }
        else
            tagsAffinity = 0f;

        if (Persons != null && other.Persons != null && Persons.Count() > 0 && other.Persons.Count() > 0)
        {
            int intersectingPersonsCount = Persons.Intersect(other.Persons).Count();
            personsAffinity = (float)intersectingPersonsCount / (Persons.Count * 6);
        }
        else
            personsAffinity = 0f;

        return tagsAffinity + personsAffinity + other.Rating / 20;
    }

    public override string ToString()
    {
        var builder = new StringBuilder($"Original Title: {OriginalTitle}\n");

        if (Persons != null && Persons.Count != 0)
        {
            foreach (var person in Persons)
            {
                builder.Append(person.Name + " = " + person.Category + '\n');
            }
        }

        if (Tags != null && Tags.Count != 0)
        {
            builder.Append("Tags count: " + Tags.Count + '\n');
        }
        else
        {
            builder.Append("Tags: no information available\n");
        }

        builder.Append("Rating: ");
        if (Rating != -1)
        {
            builder.Append(Rating);
            builder.Append('\n');
        }
        else
        {
            builder.Append("no information available\n");
        }

        if (Top != null && Top.Count != 0)
        {
            builder.Append("Top 10 Recommended Movies:\n");
            var k = 1;
            var sortedTop = Top.OrderByDescending(movie => GetAffinity(movie));
            foreach (var movie in sortedTop)
            {
                builder.Append($"{k}) {movie.OriginalTitle} | affinity {GetAffinity(movie)}\n");
                k++;
            }
        }

        return builder.ToString();
    }
}