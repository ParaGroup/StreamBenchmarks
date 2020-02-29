package brisk.execution.runtime;

// CPUAssigner class
public class CPUAssigner {
	static int index = 0;
	// for Titanic
    static final int[] CPUs = { 16, 32, 48, 8, 24, 40, 56, 2, 18, 34, 50, 10, 26, 42, 58, 4, 20, 36, 52, 12, 28, 44, 60, 6, 22, 38, 54, 14, 30, 46, 62, 1, 17, 33, 49, 9, 25, 41, 57, 3, 19, 35, 51, 11, 27, 43, 59, 5, 21, 37, 53, 13, 29, 45, 61, 7, 23, 39, 55, 15, 31, 47, 63 };
	// for Pianosa/Pianosau
	// static final int[] CPUs = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

    // method to get the next available core identifier
    public static synchronized int getNextCore() {
        // for Titanic
        if (index >= 63)
         	return -1;
        // for Pianosa/Pianosau
        // if (index >= 15)
        //	return -1;
        else
	        return CPUs[index++];
    }
}
