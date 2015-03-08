package heap;

enum LogLevel {
    VERBOSE(0), MOST(1), MORE(2), LESS(3), NONE(4);
    private int level;
    LogLevel(int level) { this.level = level; }
}

public class Log
{
   static LogLevel current = LogLevel.NONE;

   public static boolean IsVerbose() { return current == LogLevel.VERBOSE ? true : false; }

   public static void log(LogLevel level, String format, Object...args) {
       if( level.ordinal() >= Log.current.ordinal() ) {
           System.out.printf(format, args);
       }
   }
}
