using System.ComponentModel.DataAnnotations.Schema;

namespace DataBaseMovieApp.Models;

public class Title
{
    public int Id { get; set; }
    public string Name { get; set; }
    
    public Movie? Movie { get; set; }
    public int MovieId { get; set; }
}