using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

using System.Data;

namespace TC_Tests
{
    //[TestClass]
    //public class CachingTests
    //{
    //    private SLog.ISLogger _logger;
    //    private TimeCacheNetworkServer.Query.TestQuerier _querier;

    //    [TestInitialize]
    //    public void SetUpSegmentManager()
    //    {
    //        _logger = SLog.EmptySLogger.Instance;
    //        _querier = new TimeCacheNetworkServer.Query.TestQuerier();
    //    }

    //    [TestMethod]
    //    public void Overlaps_Contains_QueryRange_ShouldMatch(TimeCacheNetworkServer.QueryRange range)
    //    {
            
    //    }
    //}

    [TestClass]
    public class CachingSegmentTests
    {
        private SLog.ISLogger _logger;
        private Utils.FixedSizeBytePool _pool;
        private TimeCacheNetworkServer.Caching.CacheSegment _segment;

        private DateTime _start;
        private DateTime _end;

        [TestInitialize]
        public void SetupCacheSegment()
        {
            _logger = SLog.EmptySLogger.Instance;
            _pool = null;
            _segment = new TimeCacheNetworkServer.Caching.CacheSegment("test", _logger, _pool);

            _start = new DateTime(2022, 05, 21, 00, 00, 00);
            _end = _start.AddHours(1);
        }

        private void SetTestData()
        {
           
            TimeSpan intervalStep = TimeSpan.FromMinutes(1);
            DataTable testData = TestData.GetTestData(_start, _end, intervalStep);
            _segment.AddData(testData, _start, _end, TestData.TimeColumnIndex);
        }

        [TestMethod]
        public void SetData_VerifyStartEnd()
        {
            SetTestData();

            Assert.AreEqual(_start, _segment.DataStartTime, "Start time was not set on add");
            Assert.AreEqual(_end, _segment.DataEndTime, "End time was not set on add");

        }

        private TimeCacheNetworkServer.QueryRange GetRange(string start, string end) 
        {
            return new TimeCacheNetworkServer.QueryRange(DateTime.Parse(start), DateTime.Parse(end));
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 00:31:00")]
        public void SetData_ContainsRange_True(string start, string end)
        {
            SetTestData();

            Assert.IsTrue(_segment.Contains(GetRange(start, end)));
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 01:31:00")]
        [DataRow("2022-05-22 00:30:00", "2022-05-22 00:31:00")]
        public void SetData_ContainsRange_False(string start, string end)
        {
            SetTestData();

            Assert.IsFalse(_segment.Contains(GetRange(start, end)));
        }
    }
}
