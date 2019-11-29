using System;
using System.Collections.Generic;
using System.Text;

namespace NHibernate.BulkBatcher.Tests.Model
{
    public class TestRelatedEntity
    {
        public virtual Guid Id { get; set; }

        public virtual string Value { get; set; }

        public virtual Guid? TestEntityId { get; set; }

        public virtual TestEntity TestEntity { get; set; }
    }
}
