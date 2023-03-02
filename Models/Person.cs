using DataBaseMovieApp;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace DataBaseMovieApp.Models;

public class Person
{
    public int Id { get; set; }
    public string IdImdb { get; set; }
    public string Name { get; set; }
    public string? Category { get; set; }
    public virtual HashSet<Movie>? Movies { get; set; }

    public override string ToString()
    {
        var result = $"Name: {Name}\nCategory: {Category}\n";
        Movies.ToList().ForEach(item => result += item.ToString());
        return result;
    }
    public override int GetHashCode()
    {
        if (Name == null) 
            return 0;
        return Name.GetHashCode();
    }

    public override bool Equals(object? obj)
    {
        Person? other = obj as Person;
        return other != null && other.Name == Name;
    }
}