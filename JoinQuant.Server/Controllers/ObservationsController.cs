using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using JoinQuant.Server.Services;

namespace JoinQuant.Server.Controllers
{
    [ApiController]
    [Route("api/observations")]
    public class ObservationsController : ControllerBase
    {
        private readonly AppDbContext _db;

        public ObservationsController(AppDbContext db)
        {
            _db = db;
        }

        [HttpGet("by-series/{seriesId}")]
        public async Task<IActionResult> BySeries(string seriesId)
        {
            var data = await _db.Observations
                .Where(o => o.SeriesId == seriesId)
                .OrderByDescending(o => o.ObsDate)
                .ToListAsync();

            return Ok(data);
        }

        [HttpGet("by-channel/{channel}")]
        public async Task<IActionResult> ByChannel(string channel)
        {
            var data = await _db.Observations
                .Where(o => o.ChannelName == channel)
                .OrderByDescending(o => o.ObsDate)
                .ToListAsync();

            return Ok(data);
        }
    }
}
