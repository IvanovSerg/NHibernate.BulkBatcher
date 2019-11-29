using System;
using System.Collections.Generic;
using System.Text;

namespace NHibernate.BulkBatcher.Tests.Model
{
    public class TestMTMEntity
    {
        public virtual Guid Id { get; set; }

        public virtual string Value { get; set; }
        
        public virtual IList<TestEntity> Entities { get; set; }
    }
}
