using System;
using System.Collections.Generic;
using System.Text;

namespace NHibernate.BulkBatcher.Tests.Model
{
    public class TestEntity
    {
        public virtual Guid Id { get; set; }

        public virtual string Value { get; set; }

        public virtual IList<TestChildEntity> ChildEntities { get; set; }

        public virtual ICollection<TestRelatedEntity> RelatedEntities { get; set; }
        
        public virtual IList<TestMTMEntity> ManyToManyEntities { get; set; }
    }
}
