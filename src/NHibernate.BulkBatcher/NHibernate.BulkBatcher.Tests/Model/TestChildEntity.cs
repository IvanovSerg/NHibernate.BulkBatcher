using System;
using System.Collections.Generic;
using System.Text;

namespace NHibernate.BulkBatcher.Tests.Model
{
    public class TestChildEntity
    {
        public virtual string Value { get; set; }

        public virtual Guid? RelatedEntityId { get; set; }

        public virtual TestRelatedEntity RelatedEntity { get; set; }
    }
}
