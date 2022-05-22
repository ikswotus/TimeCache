using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;

namespace TC_Tests
{
    public abstract class CacheHelper
    {
        [TestInitialize]
        public void SetupCacheSegment()
        {
            _logger = SLog.EmptySLogger.Instance;
            _pool = null;
            _defaultSegment = new TimeCacheNetworkServer.Caching.CacheSegment("test", _logger, _pool);

            _start = new DateTime(2022, 05, 21, 01, 00, 00);
            _end = _start.AddHours(1);

            _secondStart = _end.AddHours(1);
            _secondEnd = _secondStart.AddHours(1);


            // Allow default segment to be populated for most tests
            SetTestData();
        }

        private void SetTestData()
        {

            TimeSpan intervalStep = TimeSpan.FromMinutes(1);
            DataTable testData = TestData.GetTestData(_start, _end, intervalStep);
            _defaultSegment.AddData(testData, _start, _end, TestData.TimeColumnIndex);
        }

        protected void SetSecondTestData()
        {
            _secondSegment = new TimeCacheNetworkServer.Caching.CacheSegment("test", _logger, _pool);
            TimeSpan intervalStep = TimeSpan.FromMinutes(1);
            DataTable testData = TestData.GetTestData(_secondStart, _secondEnd, intervalStep);
            _secondSegment.AddData(testData, _secondStart, _secondEnd, TestData.TimeColumnIndex);
        }

        protected TimeCacheNetworkServer.Caching.CacheSegment GetOverlappingDefaultSegment()
        {
            return GetCacheSegment(_start.AddMinutes(30), _end.AddMinutes(30));
        }

        protected TimeCacheNetworkServer.Caching.CacheSegment GetCacheSegment(DateTime start, DateTime end)
        {
            return GetCacheSegment(start, end, TimeSpan.FromMinutes(1));
        }
        protected TimeCacheNetworkServer.Caching.CacheSegment GetCacheSegment(DateTime start, DateTime end, TimeSpan intervalStep)
        {
            TimeCacheNetworkServer.Caching.CacheSegment seg = new TimeCacheNetworkServer.Caching.CacheSegment("test", _logger, _pool);

            DataTable testData = TestData.GetTestData(start, end, intervalStep);
            seg.AddData(testData, start, end, TestData.TimeColumnIndex);

            return seg;
        }

        protected SLog.ISLogger _logger;
        protected Utils.FixedSizeBytePool _pool;

        protected TimeCacheNetworkServer.Caching.CacheSegment _defaultSegment;
        protected TimeCacheNetworkServer.Caching.CacheSegment _secondSegment;

        // Covers 01:00:00 - 02:00:00
        protected DateTime _start;
        protected DateTime _end;

        // Covers 03:00:00 - 04:00:00
        protected DateTime _secondStart;
        protected DateTime _secondEnd;

        protected TimeCacheNetworkServer.QueryRange GetRange(string start, string end)
        {
            return new TimeCacheNetworkServer.QueryRange(DateTime.Parse(start), DateTime.Parse(end));
        }
    }

    [TestClass]
    public class CachingSegmentTests : CacheHelper
    {
        [TestMethod]
        public void SetData_VerifyStartEnd()
        {
            Assert.AreEqual(_start, _defaultSegment.DataStartTime, "Start time was not set on add");
            Assert.AreEqual(_end, _defaultSegment.DataEndTime, "End time was not set on add");

        }

        

        [TestMethod]
        [DataRow("2022-05-21 01:01:00", "2022-05-21 01:59:00")]// Contains
        [DataRow("2022-05-21 00:00:00", "2022-05-21 01:30:00")]// OverlapStart
        [DataRow("2022-05-21 01:30:00", "2022-05-21 02:30:00")]// OverlapEnd
        [DataRow("2022-05-20 23:30:00", "2022-05-21 02:30:00")]// Envelopes
        public void DefaultSegment_SetData_QueryOverlaps_True(string start, string end)
        {
            Assert.IsTrue(_defaultSegment.Overlaps(GetRange(start, end)));
        }

        [TestMethod]
        [DataRow("2022-05-21 00:00:00", "2022-05-21 00:59:00")]// Too Early
        [DataRow("2022-05-21 02:00:01", "2022-05-21 02:30:00")]// Too Late
        public void DefaultSegment_SetData_QueryOverlaps_False(string start, string end)
        {
            Assert.IsFalse(_defaultSegment.Overlaps(GetRange(start, end)));
        }



        [TestMethod]
        [DataRow("2022-05-21 01:30:00", "2022-05-21 01:31:00")]
        [DataRow("2022-05-21 01:00:01", "2022-05-21 01:59:59")]
        [DataRow("2022-05-21 01:00:00", "2022-05-21 02:00:00")]
        public void DefaultSegment_SetData_ContainsRange_True(string start, string end)
        {
            Assert.IsTrue(_defaultSegment.Contains(GetRange(start, end)));
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 01:31:00")]
        [DataRow("2022-05-22 00:30:00", "2022-05-22 00:31:00")]
        public void DefaultSegment_SetData_ContainsRange_False(string start, string end)
        {
            Assert.IsFalse(_defaultSegment.Contains(GetRange(start, end)));
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 03:30:00")] // Padded
        [DataRow("2022-05-21 00:59:59", "2022-05-21 02:00:01")] // Padded2
        public void DefaultSegment_SetData_Envelops_True(string start, string end)
        {
            Assert.IsTrue(_defaultSegment.Enveloped(GetRange(start, end)));
        }

        [TestMethod]
        [DataRow("2022-05-21 01:00:01", "2022-05-21 02:01:00")] // too short start
        [DataRow("2022-05-21 00:59:59", "2022-05-21 01:59:59")] // too short end
        [DataRow("2022-05-21 01:00:00", "2022-05-21 02:00:00")] // exact
        public void DefaultSegment_SetData_Envelops_False(string start, string end)
        {
            Assert.IsFalse(_defaultSegment.Enveloped(GetRange(start, end)));
        }


        [TestMethod]
        [DataRow("2022-05-21 01:30:00", "2022-05-21 02:30:00")]
        public void DefaultSegment_SetData_OverlapsStart_True(string start, string end)
        {
            Assert.IsTrue(_defaultSegment.OverlapsStart(GetRange(start, end)));
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 01:30:00")]//overlaps end
        public void DefaultSegment_SetData_OverlapsStart_False(string start, string end)
        {
            Assert.IsFalse(_defaultSegment.OverlapsStart(GetRange(start, end)));
        }


        ///
        /// INTERSECT TESTS
        ///

        [TestMethod]
        [DataRow("2022-05-21 01:30:00", "2022-05-21 01:35:00")]// contains (sub)
        [DataRow("2022-05-21 01:00:00", "2022-05-21 02:00:00")]// contains (full)
        public void DefaultSegment_Intersect_NoDataNeeded(string start, string end)
        {
            List<TimeCacheNetworkServer.QueryRange> ranges = _defaultSegment.Intersect(GetRange(start, end));
            Assert.AreEqual(0, ranges.Count);
        }

        [TestMethod]
        [DataRow("2022-05-21 00:00:00", "2022-05-21 00:59:59")]// Too early
        [DataRow("2022-05-21 02:00:01", "2022-05-21 03:00:00")]// Too Late
        [DataRow("2022-05-23 02:00:01", "2022-05-23 03:00:00")]// Nowhere close
        public void DefaultSegment_Intersect_AllDataNeeded(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);
            List<TimeCacheNetworkServer.QueryRange> ranges = _defaultSegment.Intersect(qr);
            Assert.AreEqual(1, ranges.Count);
            Assert.AreEqual(ranges[0].StartTime, qr.StartTime);
            Assert.AreEqual(ranges[0].EndTime, qr.EndTime);
        }

        [TestMethod]
        [DataRow("2022-05-21 00:00:00", "2022-05-21 01:30:00")]
        public void DefaultSegment_Intersect_StartNeeded(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);
            List<TimeCacheNetworkServer.QueryRange> ranges = _defaultSegment.Intersect(qr);
            Assert.AreEqual(1, ranges.Count);
            Assert.AreEqual(ranges[0].StartTime, qr.StartTime);
            Assert.AreEqual(ranges[0].EndTime, _start);
        }

        [TestMethod]
        [DataRow("2022-05-21 01:00:00", "2022-05-21 02:30:00")]
        public void DefaultSegment_Intersect_EndNeeded(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);
            List<TimeCacheNetworkServer.QueryRange> ranges = _defaultSegment.Intersect(qr);
            Assert.AreEqual(1, ranges.Count);
            Assert.AreEqual(ranges[0].StartTime, _end);
            Assert.AreEqual(ranges[0].EndTime, qr.EndTime);
        }


        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 02:30:00")]
        public void DefaultSegment_Intersect_StartAndEndNeeded(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);
            List<TimeCacheNetworkServer.QueryRange> ranges = _defaultSegment.Intersect(qr);
            Assert.AreEqual(2, ranges.Count);
            // First range should be 00:30:00 to 00:01:00
            Assert.AreEqual(ranges[0].StartTime, qr.StartTime);
            Assert.AreEqual(ranges[0].EndTime, _start);
            // Second range should be 00:02:00 to 00:02:30
            Assert.AreEqual(ranges[1].StartTime, _end);
            Assert.AreEqual(ranges[1].EndTime, qr.EndTime);
        }

        /// <summary>
        /// Our default segments: _defaultSegment and _secondSegment should not be merged
        /// as they do not overlap
        /// </summary>
        [TestMethod]
        public void OverlappingOrdered_VerifyGapBetweenDefaultSegments()
        {
            SetSecondTestData();

            Assert.IsFalse(_defaultSegment.OverlapsOrdered(_secondSegment.Range));
        }

        /// <summary>
        /// Create a 
        /// </summary>
        [TestMethod]
        public void OverlappingOrdered_VerifyOverlaps()
        {
            TimeCacheNetworkServer.Caching.CacheSegment segment = GetOverlappingDefaultSegment();

            Assert.IsTrue(_defaultSegment.OverlapsOrdered(segment.Range));
        }


        // TODO: 
        // MergeSegments
        // Intersect with Bucketing/Update intervals
        // GetRows? Predicates
        // Multiple Segments
    }

    [TestClass]
    public class SegmentManagerTests : CacheHelper
    {
        private TimeCacheNetworkServer.Query.TestQuerier _querier;
        private TimeCacheNetworkServer.Caching.SegmentManager _manager;

        [TestInitialize]
        public void SetUpSegmentManager()
        {
            _querier = new TimeCacheNetworkServer.Query.TestQuerier();
            _manager = new TimeCacheNetworkServer.Caching.SegmentManager(_querier, _logger, "test");

            _manager.AddSegment(_defaultSegment);
        }

        private void AddSecondSegment()
        {
            SetSecondTestData();
            _manager.AddSegment(_secondSegment);
        }


        [TestMethod]
        [DataRow("2022-05-21 01:00:00", "2022-05-21 02:00:00")] // full
        [DataRow("2022-05-21 01:00:01", "2022-05-21 01:59:59")] // partial
        [DataRow("2022-05-21 01:23:45", "2022-05-21 01:45:52")] // partial2
        public void GetMissingRanges_SingleSegment_NoRangeNeeded(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(0, missing.Count, "No missing ranges are expected");
        }

        [TestMethod]
        [DataRow("2022-05-21 00:00:00", "2022-05-21 00:59:59")] // Early
        [DataRow("2022-05-21 02:00:01", "2022-05-21 02:59:59")] // late
        public void GetMissingRanges_SingleSegment_FullRangeNeeded(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(1, missing.Count, "1 missing ranges is expected");
            Assert.AreEqual(missing[0].StartTime, qr.StartTime);
            Assert.AreEqual(missing[0].EndTime, qr.EndTime);
        }

        [TestMethod]
        [DataRow("2022-05-21 01:30:00", "2022-05-21 02:59:59")] // Start overlap
        public void GetMissingRanges_SingleSegment_PartialRangeNeeded_Start(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(1, missing.Count, "1 missing ranges is expected");
            Assert.AreEqual(missing[0].StartTime, _end);
            Assert.AreEqual(missing[0].EndTime, qr.EndTime);
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 01:59:59")] // End overlap
        public void GetMissingRanges_SingleSegment_PartialRangeNeeded_End(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(1, missing.Count, "1 missing ranges is expected");
            Assert.AreEqual(missing[0].StartTime, qr.StartTime);
            Assert.AreEqual(missing[0].EndTime, _start);
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 02:59:59")]
        [DataRow("2022-05-21 00:00:00", "2022-05-21 04:12:34")]
        public void GetMissingRanges_SingleSegment_MultipleRanges_StartEnd(string start, string end)
        {
            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(2, missing.Count, "2 missing ranges are expected");
            Assert.AreEqual(missing[0].StartTime, qr.StartTime);
            Assert.AreEqual(missing[0].EndTime, _start);
            Assert.AreEqual(missing[1].StartTime, _end);
            Assert.AreEqual(missing[1].EndTime, qr.EndTime);
        }

        [TestMethod]
        [DataRow("2022-05-21 01:30:00", "2022-05-21 03:30:30")]
        [DataRow("2022-05-21 01:00:01", "2022-05-21 03:00:01")]
        public void GetMissingRanges_MultipleSegments_SingleRangeNeeded_Gap(string start, string end)
        {
            AddSecondSegment();

            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(1, missing.Count, "1 missing range is expected");
            Assert.AreEqual(missing[0].StartTime, _end);
            Assert.AreEqual(missing[0].EndTime, _secondStart);
        }
        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 03:30:30")]
        public void GetMissingRanges_MultipleSegments_MultipleRanges_StartAndGap(string start, string end)
        {
            AddSecondSegment();

            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(2, missing.Count, "2 missing ranges are expected");
            // First range is at start
            Assert.AreEqual(missing[0].StartTime, qr.StartTime);
            Assert.AreEqual(missing[0].EndTime, _start);
            // Second range will be gap between segments
            Assert.AreEqual(missing[1].StartTime, _end);
            Assert.AreEqual(missing[1].EndTime, _secondStart);
        }

        [TestMethod]
        [DataRow("2022-05-21 00:30:00", "2022-05-21 05:30:30")]
        public void GetMissingRanges_MultipleSegments_MultipleRanges_StartAndGapAndEnd(string start, string end)
        {
            AddSecondSegment();

            TimeCacheNetworkServer.QueryRange qr = GetRange(start, end);

            List<TimeCacheNetworkServer.QueryRange> missing = _manager.GetMissingRanges(qr);

            Assert.AreEqual(3, missing.Count, "3 missing ranges are expected");

            // First range is at start
            Assert.AreEqual(missing[0].StartTime, qr.StartTime);
            Assert.AreEqual(missing[0].EndTime, _start);

            // Second range will be gap between segments
            Assert.AreEqual(missing[1].StartTime, _end);
            Assert.AreEqual(missing[1].EndTime, _secondStart);

            // last range will be end
            Assert.AreEqual(missing[2].StartTime, _secondEnd);
            Assert.AreEqual(missing[2].EndTime, qr.EndTime);
        }

        [TestMethod]
        public void MergeSegements_VerifySingleFromOverlappingDefault()
        {
            var seg = GetOverlappingDefaultSegment();

            _manager.AddSegment(seg, false);

            Assert.AreEqual(2, _manager.GetSegmentSummaries().Count);

            _manager.MergeSegments();

            Assert.AreEqual(1, _manager.GetSegmentSummaries().Count);

            TimeCacheNetworkServer.Caching.SegmentSummary summary = _manager.GetSegmentSummaries()[0];

            Assert.AreEqual(summary.Start, _start);
            Assert.AreEqual(summary.End, seg.DataEndTime);
        }
       

        // More Tests --
        // TODO: Verify rows are correct? Should account for update/overlapping
        /// --- would need different values in rows? Pick the overlapping timestamp?
    }
}
