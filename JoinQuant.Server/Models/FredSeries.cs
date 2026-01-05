using Microsoft.AspNetCore.Mvc;

namespace JoinQuant.Server.Models;

public class FredSeries
{
    public string SeriesId { get; set; }
    public string TitleCn { get; set; }
    public string Link { get; set; }

    public ICollection<Observation> Observations { get; set; }
}
