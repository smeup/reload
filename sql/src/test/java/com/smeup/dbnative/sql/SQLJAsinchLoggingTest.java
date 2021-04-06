package com.smeup.dbnative.sql;

import com.smeup.dbnative.file.Record;
import com.smeup.dbnative.log.Logger;
import com.smeup.dbnative.log.LoggingEvent;
import com.smeup.dbnative.log.LoggingLevel;
import com.smeup.dbnative.sql.utils.TestSQLDBType;
import kotlin.Unit;
import kotlin.jvm.functions.Function1;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Formatter;
import java.util.concurrent.ArrayBlockingQueue;

import static com.smeup.dbnative.sql.utils.SQLDBTestUtilsKt.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SQLJAsinchLoggingTest {

    @BeforeClass
    public static void setUp() throws Exception{
        SQLDBMManager dbManager = dbManagerForTest();
        try {
            createAndPopulateEmployeeTable(dbManager);
        }
        finally{
            dbManager.close();
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        destroyDatabase();
    }

    private String getEmployeeName(Record record){
        return record.get("FIRSTNME") + " " + record.get("LASTNAME");
    }

    @Test
    public void findRecordsIfSetllFromLastRecord() {
        SQLDBMManager dbManager = new SQLDBMManager(TestSQLDBType.HSQLDB.getConnectionConfig());
        AsynchronousLogger logger = getLogger();
        dbManager.setLogger(logger);
        try{
            SQLDBFile dbFile =dbManager.openFile(EMPLOYEE_TABLE_NAME);
            assertTrue(dbFile.setll("200340"));
            assertEquals("HELENA WONG", getEmployeeName(dbFile.readPrevious().getRecord()));
            assertEquals("MICHELLE SPRINGER", getEmployeeName(dbFile.readPrevious().getRecord()));
        }
        finally{
            dbManager.close();
            logger.stop();
        }
    }

    @NotNull
    private AsynchronousLogger getLogger(){
        return new AsynchronousLogger(LoggingLevel.ALL);
    }

    private class AsynchronousLogger extends Logger{
        private EventQueue queue;
        public AsynchronousLogger(LoggingLevel level){
            super(level, null);
            queue = new EventQueue();
            setLoggingFunction(new Function1<LoggingEvent, Unit>() {
                public Unit invoke(LoggingEvent event) {
                    queue.offer(new EventWrapper(event));
                    return null;
                }
            });
            Thread t = new Thread(new EventConsumer(queue), "Asynch loggon sample consumer");
            t.setDaemon(false);
            t.start();
        }

        public void stop(){
            queue.setStop();
        }
    }

    private class EventWrapper{
        private LoggingEvent event;
        private boolean poison;

        public EventWrapper(LoggingEvent event){
            this(event, false);
        }

        public EventWrapper(LoggingEvent event, boolean poison){
            this.event = event;
            this.poison = poison;
        }

        public LoggingEvent getEvent() {
            return event;
        }

        public boolean isPoison() {
            return poison;
        }
    }

    private class EventQueue extends ArrayBlockingQueue<EventWrapper>{
        public EventQueue(){
            super(1000);
        }

        public void setStop() {
            this.offer(new EventWrapper(null, true));
        }
    }

    private class EventConsumer implements Runnable{
        private EventQueue queue;
        public EventConsumer(EventQueue queue){
            this.queue = queue;
        }

        public void run() {
            try {
                EventWrapper ev = null;
                do{
                    ev = getQueue().take();
                    if(!ev.isPoison()){
                        consume(ev.getEvent());
                    }
                }
                while(!ev.isPoison());
            }
            catch(Throwable t){
                throw new RuntimeException(t);
            }
        }

        private void consume(LoggingEvent event){
            System.out.println(
                    new Formatter().format("[%1$2s][%2$2s][%3$2s][%4$2s] * %5$2s %6$2s aynch",
                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(event.getIssueTime()),
                            event.getEventKey().getLevel().toString(),
                            event.getEventKey().name(),
                            event.getCallerMethod(),
                            event.getMessage(),
                            event.getElapsedTime()==null?"":" (" + event.getElapsedTime() + " ms)")
            );
        }

        public EventQueue getQueue() {
            return queue;
        }
    }
}

