using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

using TimeCacheNetworkServer.Query;
using System.Text.RegularExpressions;

namespace TC_Tests
{
    /**
     * Tests for query parsing
     */
    [TestClass]
    public class ParserTests
    {
        private SLog.ISLogger _logger;
        private QueryParser _parser;

        [TestInitialize]
        public void SetupParser()
        {
            _logger = SLog.EmptySLogger.Instance;
            _parser = new QueryParser(_logger);
        }

        private readonly DateTime _defaultQueryStart = new DateTime(2022, 05, 22, 10, 00, 00, 00, DateTimeKind.Utc);
        private readonly DateTime _defaultQueryEnd = new DateTime(2022, 05, 22, 12, 00, 00, 00, DateTimeKind.Utc);

        /// <summary>
        /// Default sample query
        /// </summary>
        public static string _sampleQuery = @"
select metric, time, avg(value)
from test.data
where time BETWEEN '2022-05-22T10:00:00.000Z' AND '2022-05-22T12:00:00.00Z'
group by 1,2
order by 2 asc";

        /// <summary>
        /// Default sample query
        /// </summary>
        public static string _expectedNormalizedQuery = @"
select metric, time, avg(value)
from test.data
where time BETWEEN '###START###' AND '###END###'
group by 1,2
order by 2 asc";

        /// <summary>
        /// Verify original query remains unmodified
        /// </summary>
        [TestMethod]
        public void SimpleParse_DefaultQuery_OriginalQueryTextUnmodified()
        {
            NormalizedQuery nq = _parser.Normalize(_sampleQuery);

            Assert.AreEqual(nq.OriginalQueryText, _sampleQuery, "Original query text should not be modified by normalize()");
        }

        /// <summary>
        /// Verify our timestamps are set correctly
        /// </summary>
        [TestMethod]
        public void SimpleParse_DefaultQuery_NormalizedQueryText()
        {
            NormalizedQuery nq = _parser.Normalize(_sampleQuery);

            Assert.AreEqual(nq.NormalizedQueryText, _expectedNormalizedQuery, "Timestamps should be updated in normalized query text");
        }

        /// <summary>
        /// Verify original query remains unmodified
        /// </summary>
        [TestMethod]
        public void SimpleParse_DefaultQuery_VerifyTimestamps()
        {
            NormalizedQuery nq = _parser.Normalize(_sampleQuery);

            Assert.AreEqual(_defaultQueryStart, nq.StartTime);
            Assert.AreEqual(_defaultQueryEnd, nq.EndTime);
        }


        // TODO:
        // MetaCommands W/Options
        // Predicates
        // Interval parsing?
        // Buckets(+Update) Rounding rules
    }
}
