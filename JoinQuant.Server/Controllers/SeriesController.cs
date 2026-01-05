using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using JoinQuant.Server.Services;

namespace JoinQuant.Server.Controllers
{
    [ApiController]
    [Route("api/series")]
    public class SeriesController : ControllerBase
    {
        private readonly AppDbContext _db;

        public SeriesController(AppDbContext db)
        {
            _db = db;
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            return Ok(await _db.FredSeries.ToListAsync());
        }

        [HttpGet("{seriesId}")]
        public async Task<IActionResult> Get(string seriesId)
        {
            var series = await _db.FredSeries.FindAsync(seriesId);
            if (series == null) return NotFound();
            return Ok(series);
        }
    }
}
