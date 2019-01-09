using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using PropertyList = System.Collections.Generic.IEnumerable<System.ValueTuple<string, object>>;

namespace Wivuu.AzTableCopy
{
    public class TableEntityConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType) => 
            typeof(PropertyList).IsAssignableFrom(objectType);

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) => 
            throw new NotImplementedException();

        public override void WriteJson(JsonWriter writer, object obj, JsonSerializer serializer)
        {
            var values = obj as PropertyList;

            writer.WriteStartObject();
            {
                foreach (var (key, value) in values)
                {
                    writer.WritePropertyName(key);
                    writer.WriteValue(value);
                }
            }
            writer.WriteEndObject();
        }
    }
}