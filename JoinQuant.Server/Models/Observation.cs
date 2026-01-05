using Microsoft.AspNetCore.Mvc;

namespace JoinQuant.Server.Models;

public class Observation
{
    public long Id { get; set; }

    public string SeriesId { get; set; }
    public string Frequency { get; set; }
    public int? Limit { get; set; }

    public string ChannelName { get; set; }
    public DateTime ObsDate { get; set; }

    public decimal? Value { get; set; }
    public string ValueUnit { get; set; }

    public FredSeries Series { get; set; }
}
