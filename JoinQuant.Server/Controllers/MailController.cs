using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using JoinQuant.Server.Services;

namespace JoinQuant.Server.Controllers
{
    [ApiController]
    [Route("api/mail")]
    public class MailController : ControllerBase
    {
        private readonly AppDbContext _db;

        public MailController(AppDbContext db)
        {
            _db = db;
        }

        [HttpGet("latest")]
        public async Task<IActionResult> Latest()
        {
            var rows = await _db.Observations
                .Where(o => o.ChannelName == "mail")
                .Include(o => o.Series)
                .OrderBy(o => o.SeriesId)
                .ThenByDescending(o => o.ObsDate)
                .Select(o => new {
                    o.SeriesId,
                    o.Series.TitleCn,
                    o.Frequency,
                    o.ObsDate,
                    o.Value,
                    o.ValueUnit
                })
                .ToListAsync();

            return Ok(rows);
        }
    }
}
