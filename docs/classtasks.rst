Class Tasks
-----------

"Class Tasks" are tasks defined as a class method.
They provide very basic primitive method of serialization and deserialization
when sending and receiving tasks.

It is better to explain with an example:

.. code-block:: python

    from kuyruk import Kuyruk

    kuyruk = Kuyruk()

    # This is your base database model
    class Base(object):

        # This is the primary key
        id = Column(Integer)

        # You need to override this function to return
        # the instance by fetching from database
        @classmethod
        def get(cls, id):
            # Something like this:
            return session.query(cls).get(id)

    class Mp4(Base):

        # some fields...
        user_id = Column(Integer)
        file_id = Column(Integer)
        created_at = Column(Datetime)

        @kuyruk.task
        def convert(self, resolution='480p'):
            print "Converting MP4(%s)..." % resolution
            # Do the actual conversion here

    # Lets create an Mp4 instance
    mp4 = Mp4(user_id=6, file_id=1234)

    # The instance must be persisted to database befor sending to queue
    session.add(mp4)
    session.commit()

    print mp4.id
    >>> 34

    # Calling task method will send a message to the queue
    mp4.convert(resolution='720p')


When you run the worker it will try to fetch the instance by
calling ``Mp4.get(34)``. Then it will call ``mp4.convert(resolution='720p')``.
