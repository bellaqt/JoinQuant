using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using JoinQuant.Server.Services;

namespace JoinQuant.Server.Controllers
{
    [ApiController]
    [Route("web/series")]
    public class WebSeriesController : Controller
    {
        private readonly AppDbContext _db;

        public WebSeriesController(AppDbContext db)
        {
            _db = db;
        }

        [HttpGet("{seriesId}")]
        public async Task<IActionResult> Index(string seriesId)
        {
            var rows = await _db.Observations
                .Where(o => o.SeriesId == seriesId && o.ChannelName == "web")
                .OrderByDescending(o => o.ObsDate)
                .ToListAsync();

            if (!rows.Any())
                return Content("No web data available.");

            ViewBag.SeriesId = seriesId;
            return View(rows);
        }
    }
}