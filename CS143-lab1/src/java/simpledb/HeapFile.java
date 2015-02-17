package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File myfile;
    private TupleDesc mytd;
    private int m_num_total_pages;

    
    public class HeapFileIterator implements DbFileIterator
    {
        private boolean m_open;
        private int m_page_number;
        private TransactionId m_tid;
        private HeapPage m_page;
        private Iterator<Tuple> m_it;

        public HeapFileIterator(TransactionId tid)
        {
            m_open = false;
            // m_page_number = 0;
            m_tid = tid;
        }

        public void open() throws TransactionAbortedException, DbException
        {
            if (m_open)
                return; // We're already open

            m_open = true;

            // Initialize m_it
            m_page_number = 0; // This should not be removed
            int table_id = getId();
            HeapPageId h_id = new HeapPageId(table_id, m_page_number);
            // The line below causes a null pointer exception
            m_page = (HeapPage)Database.getBufferPool().getPage(m_tid, h_id, Permissions.READ_WRITE);

            m_it = m_page.iterator();
            return;
        }
        public void rewind() throws TransactionAbortedException, DbException
        {
            close();
            open();
            return;
        }
        public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException
        {
            if (!hasNext() )
            {
                throw new NoSuchElementException("Iterator has no next");
            }

            if (m_it.hasNext() )
                return m_it.next();
            // We know there must be a next element, so it must be on the
            // next page
            m_page_number++;
            int table_id = getId();
            HeapPageId h_id = new HeapPageId(table_id, m_page_number);
            m_page = (HeapPage)Database.getBufferPool().getPage(m_tid, h_id, Permissions.READ_WRITE);
            m_it = m_page.iterator();

            if (!m_it.hasNext() )
                throw new NoSuchElementException("Iterator has no next");

            return m_it.next();
        }
        public boolean hasNext() throws NoSuchElementException, TransactionAbortedException, DbException
        {
            // If we're closed, we don't have a next
            if (!m_open)
                return false;
            // If we have a next, return true
            if (m_it.hasNext() )
                return true;
            // If we're not on the last page, return true
            if (m_page_number < m_num_total_pages-1)
                return true;

            // We therefore must be at the last page and at the last
            // iterator
            return false;
        }
        public void close()
        {
            m_open = false;
            return;
        }

    }


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        myfile = f;
        mytd = td;
        // Set total number of pages to the file size divided by the page
        // size (and round up)
        m_num_total_pages = (int)(f.length() / BufferPool.getPageSize() );
        m_num_total_pages = (int)Math.ceil(m_num_total_pages);
        
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return myfile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode(). "ok"
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code went here
        return myfile.getAbsoluteFile().hashCode();

    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return mytd;

    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int size_of_page = BufferPool.getPageSize();

        // Translate page id to page number
        int page_number = pid.pageNumber();

        // Skip ahead by the appropriate offset and read in 1 size_of_page
        int offset = size_of_page * page_number;

        // Now read in the file byte by byte
        byte[] buf = new byte[size_of_page];
        try
        {
            InputStream str = new BufferedInputStream(new FileInputStream(myfile) );
            str.skip(offset);
            str.read(buf);
            str.close();
            return new HeapPage( (HeapPageId)pid, buf);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	PageId pageid = page.getId();    	
    	int offset = pageid.pageNumber() * BufferPool.PAGE_SIZE;
    	RandomAccessFile raf = new RandomAccessFile(myfile, "rw");
    	
    	raf.seek(offset);
    	raf.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
    	raf.close();
    }
    

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)myfile.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	//Get first free page in database bufferpool getpage
    	//return as arraylist the page with inserted tuple
    	HeapPage heappage= null;
    	for (int i = 0; i < numPages(); i++)
    	{
    		PageId pid = new HeapPageId(getId(), i);
    		heappage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if (heappage.getNumEmptySlots() > 0)
        		break;
    	}
    	
    	// add tuple depending on free page space
    	if (heappage != null){
    		heappage.insertTuple(t);
    	}
    	else{
	        HeapPageId newId = new HeapPageId(getId(), numPages());
	        heappage = new HeapPage(newId, HeapPage.createEmptyPageData());
	        heappage.insertTuple(t);
	        
	        int offset = numPages() * BufferPool.PAGE_SIZE;
	        RandomAccessFile raf = new RandomAccessFile(myfile, "rw");
	        
	        raf.seek(offset);
	        raf.write(heappage.getPageData(), 0, BufferPool.PAGE_SIZE);
	        raf.close();
    	}
    	return new ArrayList<Page>(Arrays.asList(heappage));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	//get page, delete it, return page with deleted tuple
    	PageId pid = t.getRecordId().getPageId();
    	HeapPage heappage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	heappage.deleteTuple(t);
    	return new ArrayList<Page>(Arrays.asList(heappage));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

