package edu.mcgill.disl.log;



import java.io.*;


public class KTrace
{

    private	static	FileOutputStream		outputFile	;
    private	static	OutputStreamWriter		writer		;
    private	static	BufferedWriter			bufferedWriter	;
    private	static	PrintWriter				printWriter	;


	static
	{
		try
		{
		    outputFile		=	new FileOutputStream	( FileDescriptor.out );
		    writer			=	new OutputStreamWriter	( outputFile , "8859_1" );
		    bufferedWriter	=	new BufferedWriter	( writer );
		    printWriter		=	new PrintWriter		( bufferedWriter , true );
		}
		catch ( Exception ex )
		{
			System.out.println ( "KTrace : ex = " + ex );
		}

		open ( "KTrace.txt" );
	}

	public	static	void open ( String trace_file )
	{

		try
		{
		    outputFile		=	new FileOutputStream	( trace_file );
		    writer			=	new OutputStreamWriter	( outputFile , "8859_1" );
		    bufferedWriter	=	new BufferedWriter	( writer );
		    printWriter		=	new PrintWriter		( bufferedWriter , true );
		}
		catch ( Exception ex )
		{
			System.out.println ( "KTrace : ex = " + ex );
		}

	}

    // Close the writer, which also closes the underlying stream
	public	synchronized	static	void	close()
	{
		printWriter.close();
	}



    /** Print a String, and then finish the line. */
    public synchronized static void println ( String str )
    {
		String	threadName	=	Thread.currentThread().getName() ;

	//	printWriter.println ( "" + threadName + " : " + KProgressTime.getCurrentTime() + " : " + str );

		printWriter.println ( str );


    }


    public static void println_empty ( String str )
    {

    }


}



