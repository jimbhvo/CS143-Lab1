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
    File myfile;
    TupleDesc mytd;

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
            m_page_number = 0;
            m_tid = tid;
        }

        public void open()
        {
            if (m_open)
                return; // We're already open

            m_open = true;
            return;
        }
        public void rewind()
        {
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
            return null;
        }
        public boolean hasNext() throws NoSuchElementException, TransactionAbortedException, DbException
        {
            if (!m_open)
                return false; // If we're closed, we don't have a next

            if (m_it.hasNext() )
                return true;
            else
            {
                // We're either at the end of a page or there is no next
                m_page_number++;
                int table_id = getId();
                HeapPageId h_id = new HeapPageId(table_id, m_page_number);
                m_page = (HeapPage)Database.getBufferPool().getPage(m_tid, h_id, Permissions.READ_WRITE);
                return false;
            }
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
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
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
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

