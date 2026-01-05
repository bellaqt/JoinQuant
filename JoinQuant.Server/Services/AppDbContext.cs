using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using JoinQuant.Server.Models;

namespace JoinQuant.Server.Services;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options)
        : base(options) { }

    public DbSet<FredSeries> FredSeries { get; set; }
    public DbSet<Observation> Observations { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {

        modelBuilder.Entity<FredSeries>(entity =>
        {
            entity.ToTable("fred_series");
            entity.HasKey(e => e.SeriesId);

            entity.Property(e => e.SeriesId)
                  .HasColumnName("series_id");

            entity.Property(e => e.TitleCn)
                  .HasColumnName("title_cn");

            entity.Property(e => e.Link)
                  .HasColumnName("link");
        });

        modelBuilder.Entity<Observation>(entity =>
        {
            entity.ToTable("observations");

            entity.HasKey(e => e.Id);

            entity.Property(e => e.Id)
                  .HasColumnName("id");

            entity.Property(e => e.SeriesId)
                  .HasColumnName("series_id");

            entity.Property(e => e.Frequency)
                  .HasColumnName("frequency");

            entity.Property(e => e.Limit)
                  .HasColumnName("limit");

            entity.Property(e => e.ChannelName)
                  .HasColumnName("channel_name");

            entity.Property(e => e.ObsDate)
                  .HasColumnName("obs_date");

            entity.Property(e => e.Value)
                  .HasColumnName("value");

            entity.Property(e => e.ValueUnit)
                  .HasColumnName("value_unit");

            entity.HasOne(o => o.Series)
                  .WithMany(s => s.Observations)
                  .HasForeignKey(o => o.SeriesId);
        });
    }

    public IQueryable<FredSeries> QuerySeries()
        => FredSeries.AsNoTracking();

    public IQueryable<Observation> QueryObservationsBySeries(string seriesId)
        => Observations
            .AsNoTracking()
            .Where(o => o.SeriesId == seriesId)
            .OrderByDescending(o => o.ObsDate);

    public IQueryable<Observation> QueryObservationsByChannel(string channel)
        => Observations
            .AsNoTracking()
            .Where(o => o.ChannelName == channel)
            .OrderByDescending(o => o.ObsDate);

    public IQueryable<object> QueryMailLatest()
        => Observations
            .AsNoTracking()
            .Where(o => o.ChannelName == "web")
            .Include(o => o.Series)
            .OrderBy(o => o.SeriesId)
            .ThenByDescending(o => o.ObsDate)
            .Select(o => new
            {
                o.SeriesId,
                o.Series.TitleCn,
                o.Frequency,
                o.ObsDate,
                o.Value,
                o.ValueUnit
            });
}