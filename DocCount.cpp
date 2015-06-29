#include <MarkLogic.h>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include <sstream>
#include <map>

#ifdef _MSC_VER
#define PLUGIN_DLL __declspec(dllexport)
#else // !_MSC_VER
#define PLUGIN_DLL
#endif

using namespace marklogic;

////////////////////////////////////////////////////////////////////////////////

class DocCount : public AggregateUDF {

  public:
    // Counts per uri
    std::map<String, uint64_t> uris;

  public:
    DocCount() {
    }

    AggregateUDF* clone() const { return new DocCount(*this); }
    void close() { delete this; }

    void start(Sequence&, Reporter&);
    void finish(OutputSequence& os, Reporter& reporter);

    void map(TupleIterator& values, Reporter& reporter);
    void reduce(const AggregateUDF* _o, Reporter& reporter);

    void encode(Encoder& e, Reporter& reporter);
    void decode(Decoder& d, Reporter& reporter);

};

void DocCount::start(Sequence& arg, Reporter& reporter) {

  reporter.log(Reporter::Info, "DocCount start()");

}

void DocCount::finish(OutputSequence& os, Reporter& reporter) {
  
  reporter.log(Reporter::Info, "DocCount finish()");

  std::map<String,uint64_t>::iterator it = uris.begin();
  
  os.startMap();
  for (; it != uris.end(); ++it) {
    os.writeMapKey(it->first);
    os.writeValue(it->second);
  }
  os.endMap();
  
}

void DocCount::map(TupleIterator& values, Reporter& reporter) {
  
  if (values.width() != 2) {
    reporter.error("Unexpected number of range indexes.");
    // does not return
  }
  
  for(; !values.done(); values.next()) {
    if(!values.null(0) && !values.null(1)) {
      String uri;

      values.value(0, uri);
      if (uris[uri]) {
        uris[uri] = uris[uri] + values.frequency();
      } else {
        uris[uri] = values.frequency();
      }
    }
  }

}

void DocCount::reduce(const AggregateUDF* _o, Reporter& reporter) {

  const DocCount*o = (const DocCount*)_o;

  std::map<String,uint64_t>::const_iterator it = o->uris.begin();
  
  for (; it != o->uris.end(); ++it) {
    String key = it->first;

    // log << "reducing " << key;
    // reporter.log(Reporter::Info, log.str().c_str());

    if (uris[key]) {
      uris[key] = uris[key] + it->second;
    } else {
      uris[key] = it->second;
    }
  }
  
}

void DocCount::encode(Encoder& e, Reporter& reporter) {

  uint64_t size = uris.size();
  e.encode(size);
  
  std::map<String,uint64_t>::iterator it = uris.begin();
  
  for (; it != uris.end(); ++it) {
    e.encode(it->first);
    e.encode(it->second);
  }
  
}

void DocCount::decode(Decoder& d, Reporter& reporter) {
  
  uint64_t size;

  d.decode(size);

  for (uint64_t i = 0; i < size; ++i) {
    String key;
    uint64_t value;
    d.decode(key);
    d.decode(value);
    uris[key] = value;
  }
  
}

////////////////////////////////////////////////////////////////////////////////

extern "C" PLUGIN_DLL void marklogicPlugin(Registry& r) {
  r.version();
  r.registerAggregate<DocCount>("doc-count");
}
