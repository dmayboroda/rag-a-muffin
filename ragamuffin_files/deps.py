from ragamuffin_core.async_queue import AsyncQueue
"""
The `file_queue` instance will be used throughout the application to enqueue and 
process file-related tasks asynchronously.
"""
file_queue = AsyncQueue()