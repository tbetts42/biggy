using System;
using System.Collections.Generic;
using System.Linq;
using Biggy.Mongo.Tests.Support;
using Xunit;

namespace Biggy.Mongo.Tests
{
    public class MongoyList_Tests
    {
        public MongoyList_Tests()
        {
            _dbVerifier.Clear();
        }

        [Fact(DisplayName = "Mongo: loads empty list into memory")]
        public void LoadsEmpty()
        {
            var widgets = new MongoyList<Widget>(Host, Database, Collection);
            Assert.Empty(widgets);
        }

        [Fact(DisplayName = "Mongo: loads a single document into memory")]
        public void LoadSingleDocument()
        {
            var widget = new Widget()
                {
                    Description = "A widget",
                    Expiration = DateTime.Now.AddYears(1),
                    Name = "Widget",
                    Price = 9.99m,
                    Size = 2
                };
            _dbVerifier.Insert(widget);

            var widgets = new MongoyList<Widget>(Host, Database, Collection);
            Assert.Equal(1, widgets.Count);
            
            var found = widgets.First();            
            Assert.Equal(widget.Description, found.Description);
            Assert.Equal(widget.Expiration.Year, found.Expiration.Year);
            Assert.Equal(widget.Name, found.Name);
            Assert.Equal(widget.Price, found.Price);
            Assert.Equal(widget.Size, found.Size);
        }

        [Fact(DisplayName = "Mongo: writes 12 metric crap-loads of records into memory and db")]
        public void WriteALot()
        {
            var data = new List<Widget>();
            for (var i = 0; i < 10000; i++)
            {
                data.Add(new Widget
                    {
                        Description = "A widget",
                        Expiration = DateTime.Now.AddYears(1),
                        Name = "Widget",
                        Price = 9.99m,
                        Size = i
                    });
            }

            var widgets = new MongoyList<Widget>(Host, Database, Collection);
            widgets.Add(data);

            Assert.Equal(data.Count, widgets.Count);
            Assert.Equal(data.Count, _dbVerifier.Count());
        }

        [Fact(DisplayName = "Mongo: queries a range of records from memory")]
        public void Query()
        {
            var data = new List<Widget>();
            for (var i = 0; i < 10; i++)
            {
                data.Add(new Widget
                {
                    Description = "A widget",
                    Expiration = DateTime.Now.AddYears(1),
                    Name = "Widget",
                    Price = 9.99m,
                    Size = i
                });
            }

            var widgets = new MongoyList<Widget>(Host, Database, Collection);
            widgets.Add(data);

            var query = from w in widgets
                        where w.Size > 5 && w.Size < 8
                        select w;

            Assert.Equal(2, query.Count());            
        }

        [Fact(DisplayName = "Mongo: Flush() syncs list to mongo in brute force fashion")]
        public void Update()
        {
            var widget = new Widget()
            {
                Description = "A widget",
                Expiration = DateTime.Now.AddYears(1),
                Name = "Widget",
                Price = 9.99m,
                Size = 2
            };

            var widgets = new MongoyList<Widget>(Host, Database, Collection);
            widgets.Add(widget);
            
            widget.Name = "I updated this!!";
            widgets.Flush();

            var updatedWidget = _dbVerifier.Find(widget.Id);
            Assert.Equal(widget.Name, updatedWidget.Name);
        }
        
        [Fact(DisplayName = "Mongo: Update() properly updates item in mongo collection")]
        public void UpdateInMongo()
        {
            // first create a widget and add it to the collection and mongodb
            var widget = new Widget()
            {
                Description = "Widget number 42",
                Expiration = DateTime.Now.AddYears(1),
                Name = "Widget42",
                Price = 9.99m,
                Size = 2
            };

            var widgets = new MongoyList<Widget>(Host, Database, Collection);
            widgets.Add(widget);
            widgets.Flush();
            // find by name
            var widgetByName = widgets.SingleOrDefault(w => w.Name == "Widget42");
            var updatedWidget = _dbVerifier.Find(widgetByName.Id);
            Assert.Equal(widget.Price, updatedWidget.Price);

            // update it again
            widgetByName.Price= 42.0m;
            widgets.Flush();
            var widgetByName2 = widgets.SingleOrDefault(w => w.Name == "Widget42");

            // ID should be the same
            Assert.Equal(widgetByName.Id, widgetByName2.Id);
            var updatedWidget2 = _dbVerifier.Find(widgetByName2.Id);
            Assert.Equal(42.0m, widgetByName2.Price);
            Assert.Equal(42.0m, updatedWidget2.Price);
            Assert.Equal(widgetByName.Price, updatedWidget2.Price);
        }

        [Fact(DisplayName = "Mongo: deletes a single record in memory and db")]
        public void Delete()
        {
            var widget = new Widget()
            {
                Description = "A widget",
                Expiration = DateTime.Now.AddYears(1),
                Name = "Widget",
                Price = 9.99m,
                Size = 2
            };

            var widgets = new MongoyList<Widget>(Host, Database, Collection);
            widgets.Add(widget);
            widgets.Remove(widget);           
            
            Assert.Equal(0, _dbVerifier.Count());
        }

        private const string Host = "localhost";
        private const string Database = "biggytest";
        private const string Collection = "widgets";
        private readonly MongoHelper<Widget> _dbVerifier = 
            new MongoHelper<Widget>(Host, Database, Collection);                                     
    }
}