package com.goibibo.filebeat_flume_interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.nio.charset.Charset;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.DateTime;
import org.apache.flume.event.SimpleEvent;

public class FilebeatInterceptor implements Interceptor {
  private static final Logger logger = LoggerFactory
          .getLogger(FilebeatInterceptor.class);
  private static final DateTimeFormatter isoDateTimeFormat = ISODateTimeFormat.dateTime();
  private static final Charset utf8 = Charset.forName("UTF-8");
  private final Boolean preserveExisting;
  private static final  String  CONF_PRESERVE_EXISTING    = "preserveExisting";
  public  static final  String  CONF_TIMESTAMP            = "timestamp";
  private static final  Boolean DEFAULT_PRESERVE_EXISTING = false;

  private final Gson gson;
  private FilebeatInterceptor(Boolean preserveExisting) {
    gson = new Gson();
    this.preserveExisting = preserveExisting;
  }

  @Override
  public void initialize() {
    //No op
  }

  @Override
  public Event intercept(final Event event) {
    try {
      String body = new String(event.getBody(), utf8);
      if(body.length() > 0 && body.charAt(0) == '{') {
        makeEventKafkaTailerCompatible(event, body);
        logger.debug("FileBeat message converted from {}", body);
        logger.debug("to {}", new String(event.getBody(),utf8));
        logger.debug("Timestamp is {}", event.getHeaders());
      } else {
        //No op, Event from Kafka-tailer, Return it untouched
        logger.debug("Message is not from FileBeat {}", body);
      }
    } catch (JsonSyntaxException e) {
      logger.warn("Could not parse JSON. Exception follows. {}", e);
    } catch(java.lang.IllegalArgumentException e) {
      logger.warn("Could not parse Timestamp. Exception follows. {}", e);
      return null;
    } catch (Throwable e) {
      logger.warn("Unhandled error. Exception follows. {}", e);
    }
    return event;
  }

  @Override
  public List<Event> intercept(final List<Event> events) {
    for (Event e : events) {
      intercept(e); //ignore the return value, the event is modified in place.
    }
    return events;
  }

  @Override
  public void close() {
    // No op
  }

  private void makeEventKafkaTailerCompatible(final Event event, final String body) {
    final FileBeatEvent fileBeatEvent = gson.fromJson(body,FileBeatEvent.class);
    event.setBody(fileBeatEvent.formatToEventString().getBytes(utf8));
    if(event.getHeaders().containsKey(CONF_TIMESTAMP) && preserveExisting){
      //No op, Preserve the timestamp
    } else {
      event.getHeaders().put(CONF_TIMESTAMP, Long.toString(fileBeatEvent.getTimestampInMillis()));
    }
  }

  public static class FileBeatEventHostDetails {
    private String hostname;
    private String name;
  }
  //{"@timestamp":"2016-07-09T11:40:05.684Z","beat":{"hostname":"nmlgodataplat03","name":"nmlgodataplat03"},"message":"128","type":"log"}
  public static class FileBeatEvent {
     @SerializedName("@timestamp") private String timestamp;
     private FileBeatEventHostDetails beat;
     private String message;
     public String formatToEventString() {
      return beat.name + "\t" + message + "\t" + beat.hostname;
     }
     public long getTimestampInMillis() {
      return DateTime.parse(timestamp,isoDateTimeFormat).getMillis();
     }
  }

  public static class FilebeatInterceptorBuilder
    implements Interceptor.Builder {

    private Context ctx;
    @Override
    public Interceptor build() {
      Boolean preserveExisting = ctx.getBoolean(CONF_PRESERVE_EXISTING, DEFAULT_PRESERVE_EXISTING);
      return new FilebeatInterceptor(preserveExisting);
    }

    @Override
    public void configure(Context context) {
      this.ctx = context;
    }
  }

  //For a simple manual testing.
  public static void main(String[] args) {
    logger.info("Hello World!");
    String[] messages = {"{\"@timestamp\":\"2016-07-09T01:01:01.001Z\",\"beat\":{\"hostname\":\"nmlgodataplat03\",\"name\":\"amigo-www\"},\"message\":\"128\",\"type\":\"log\"}",
                        "amigo-www\t121\tnmlgodataplat03"};
    FilebeatInterceptor fbi = new FilebeatInterceptor(false);
    for( int i =0; i<messages.length; i++) {
      SimpleEvent simpleEvent = new SimpleEvent();
      simpleEvent.setBody(messages[i].getBytes(utf8));
      fbi.intercept(simpleEvent);
    }
  }
}
