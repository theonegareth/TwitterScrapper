import json
import csv
import asyncio
import logging
from typing import Any, Dict, List, Optional, AsyncGenerator, Union
from pathlib import Path
from datetime import datetime
import aiofiles

logger = logging.getLogger(__name__)

class StreamingExporter:
    """
    Streaming exporter for large datasets.
    
    Provides memory-efficient export of large tweet collections to JSON, CSV,
    and other formats by streaming data instead of loading everything into memory.
    """
    
    def __init__(self, chunk_size: int = 1000, compression: Optional[str] = None):
        """
        Initialize streaming exporter.
        
        Args:
            chunk_size: Number of records to process in each chunk
            compression: Compression algorithm ('gzip', 'lz4', or None)
        """
        self.chunk_size = chunk_size
        self.compression = compression
        
        # Statistics
        self.records_exported = 0
        self.bytes_written = 0
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    async def export_json_stream(
        self,
        data_source: AsyncGenerator[Dict[str, Any], None],
        output_path: Union[str, Path],
        pretty_print: bool = False
    ) -> Dict[str, Any]:
        """
        Export data to JSON file using streaming.
        
        Args:
            data_source: Async generator that yields data records
            output_path: Output file path
            pretty_print: Whether to format JSON with indentation
            
        Returns:
            Dictionary with export statistics
        """
        self.start_time = datetime.now()
        self.records_exported = 0
        self.bytes_written = 0
        
        output_path = Path(output_path)
        
        try:
            # Open file asynchronously
            async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
                # Write opening bracket for JSON array
                await f.write('[\n')
                
                first_record = True
                chunk = []
                
                async for record in data_source:
                    chunk.append(record)
                    self.records_exported += 1
                    
                    # Process chunk when it reaches chunk_size
                    if len(chunk) >= self.chunk_size:
                        await self._write_json_chunk(f, chunk, first_record, pretty_print)
                        self.bytes_written += sum(len(json.dumps(r, default=str)) for r in chunk)
                        chunk = []
                        first_record = False
                        
                        # Log progress
                        if self.records_exported % (self.chunk_size * 10) == 0:
                            logger.info(f"Exported {self.records_exported} records to JSON")
                
                # Write remaining records
                if chunk:
                    await self._write_json_chunk(f, chunk, first_record, pretty_print)
                    self.bytes_written += sum(len(json.dumps(r, default=str)) for r in chunk)
                
                # Write closing bracket
                await f.write('\n]')
            
            self.end_time = datetime.now()
            
            stats = self._get_stats()
            logger.info(f"JSON export completed: {stats}")
            
            return stats
            
        except Exception as e:
            logger.error(f"JSON streaming export failed: {e}")
            raise
    
    async def _write_json_chunk(
        self,
        file_handle: Any,
        chunk: List[Dict[str, Any]],
        first_record: bool,
        pretty_print: bool
    ):
        """Write a chunk of JSON records."""
        indent = 2 if pretty_print else None
        
        for i, record in enumerate(chunk):
            if not first_record or i > 0:
                await file_handle.write(',\n')
            
            json_str = json.dumps(record, indent=indent, default=str)
            await file_handle.write('  ' + json_str if pretty_print else json_str)
    
    async def export_csv_stream(
        self,
        data_source: AsyncGenerator[Dict[str, Any], None],
        output_path: Union[str, Path],
        fieldnames: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Export data to CSV file using streaming.
        
        Args:
            data_source: Async generator that yields data records
            output_path: Output file path
            fieldnames: List of fieldnames (auto-detected if not provided)
            
        Returns:
            Dictionary with export statistics
        """
        self.start_time = datetime.now()
        self.records_exported = 0
        self.bytes_written = 0
        
        output_path = Path(output_path)
        csv_writer = None
        
        try:
            async with aiofiles.open(output_path, 'w', newline='', encoding='utf-8') as f:
                # We need to use a custom CSV writer that works with async files
                # For now, we'll buffer writes
                buffer = []
                
                async for record in data_source:
                    if csv_writer is None:
                        # Detect fieldnames from first record
                        if fieldnames is None:
                            fieldnames = list(record.keys())
                        
                        # Write header
                        header = ','.join(self._escape_csv_field(name) for name in fieldnames)
                        await f.write(header + '\n')
                        self.bytes_written += len(header) + 1
                    
                    # Write record
                    values = [self._escape_csv_field(str(record.get(field, ''))) for field in fieldnames]
                    row = ','.join(values)
                    
                    buffer.append(row + '\n')
                    self.records_exported += 1
                    self.bytes_written += len(row) + 1
                    
                    # Flush buffer periodically
                    if len(buffer) >= self.chunk_size:
                        await f.write(''.join(buffer))
                        buffer = []
                        
                        # Log progress
                        if self.records_exported % (self.chunk_size * 10) == 0:
                            logger.info(f"Exported {self.records_exported} records to CSV")
                
                # Flush remaining buffer
                if buffer:
                    await f.write(''.join(buffer))
            
            self.end_time = datetime.now()
            
            stats = self._get_stats()
            logger.info(f"CSV export completed: {stats}")
            
            return stats
            
        except Exception as e:
            logger.error(f"CSV streaming export failed: {e}")
            raise
    
    def _escape_csv_field(self, field: str) -> str:
        """Escape CSV field to handle commas and quotes."""
        field = field.replace('"', '""')
        
        if ',' in field or '"' in field or '\n' in field:
            return f'"{field}"'
        
        return field
    
    async def export_jsonl_stream(
        self,
        data_source: AsyncGenerator[Dict[str, Any], None],
        output_path: Union[str, Path]
    ) -> Dict[str, Any]:
        """
        Export data to JSONL (JSON Lines) file using streaming.
        
        Each line is a separate JSON object, which is more memory-efficient
        for large datasets than a single JSON array.
        
        Args:
            data_source: Async generator that yields data records
            output_path: Output file path
            
        Returns:
            Dictionary with export statistics
        """
        self.start_time = datetime.now()
        self.records_exported = 0
        self.bytes_written = 0
        
        output_path = Path(output_path)
        
        try:
            async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
                async for record in data_source:
                    json_line = json.dumps(record, default=str) + '\n'
                    await f.write(json_line)
                    
                    self.records_exported += 1
                    self.bytes_written += len(json_line)
                    
                    # Log progress
                    if self.records_exported % (self.chunk_size * 10) == 0:
                        logger.info(f"Exported {self.records_exported} records to JSONL")
            
            self.end_time = datetime.now()
            
            stats = self._get_stats()
            logger.info(f"JSONL export completed: {stats}")
            
            return stats
            
        except Exception as e:
            logger.error(f"JSONL streaming export failed: {e}")
            raise
    
    async def export_from_database(
        self,
        db_query: str,
        db_params: tuple,
        output_path: Union[str, Path],
        format: str = 'json',
        fieldnames: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Export data directly from database using streaming.
        
        Args:
            db_query: SQL query to execute
            db_params: Query parameters
            output_path: Output file path
            format: Export format ('json', 'csv', 'jsonl')
            fieldnames: Fieldnames for CSV export
            
        Returns:
            Dictionary with export statistics
        """
        from database import DatabaseManager
        
        async def data_generator():
            db = DatabaseManager()
            
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(db_query, db_params)
                
                # Get column names
                columns = [description[0] for description in cursor.description]
                
                while True:
                    rows = cursor.fetchmany(self.chunk_size)
                    if not rows:
                        break
                    
                    for row in rows:
                        yield dict(zip(columns, row))
        
        # Choose export format
        if format.lower() == 'json':
            return await self.export_json_stream(data_generator(), output_path)
        elif format.lower() == 'csv':
            return await self.export_csv_stream(data_generator(), output_path, fieldnames)
        elif format.lower() == 'jsonl':
            return await self.export_jsonl_stream(data_generator(), output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _get_stats(self) -> Dict[str, Any]:
        """
        Get export statistics.
        
        Returns:
            Dictionary with export statistics
        """
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'records_exported': self.records_exported,
            'bytes_written': self.bytes_written,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration,
            'average_records_per_second': (
                self.records_exported / duration if duration and duration > 0 else 0
            ),
            'average_bytes_per_record': (
                self.bytes_written / self.records_exported if self.records_exported > 0 else 0
            ),
            'chunk_size': self.chunk_size,
            'compression': self.compression
        }


class AsyncDataSource:
    """
    Async data source for streaming data from various sources.
    """
    
    @staticmethod
    async def from_list(data: List[Dict[str, Any]], chunk_size: int = 100) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Create async generator from a list.
        
        Args:
            data: List of data records
            chunk_size: Number of records to yield at a time
            
        Yields:
            Data records
        """
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            for record in chunk:
                yield record
            
            # Small delay to allow other tasks to run
            await asyncio.sleep(0)
    
    @staticmethod
    async def from_database(
        query: str,
        params: tuple,
        chunk_size: int = 100
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Create async generator from database query.
        
        Args:
            query: SQL query
            params: Query parameters
            chunk_size: Number of rows to fetch at a time
            
        Yields:
            Data records
        """
        from database import DatabaseManager
        
        db = DatabaseManager()
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                for row in rows:
                    yield dict(zip(columns, row))
                
                # Small delay to allow other tasks to run
                await asyncio.sleep(0)
    
    @staticmethod
    async def from_json_file(
        filepath: Union[str, Path],
        chunk_size: int = 100
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Create async generator from JSON file.
        
        Args:
            filepath: Path to JSON file
            chunk_size: Number of records to read at a time
            
        Yields:
            Data records
        """
        filepath = Path(filepath)
        
        async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
            # For large JSON arrays, we'd need a streaming JSON parser
            # For now, we'll assume the file contains a JSON array
            content = await f.read()
            data = json.loads(content)
            
            if isinstance(data, list):
                async for record in AsyncDataSource.from_list(data, chunk_size):
                    yield record
    
    @staticmethod
    async def from_csv_file(
        filepath: Union[str, Path],
        chunk_size: int = 100
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Create async generator from CSV file.
        
        Args:
            filepath: Path to CSV file
            chunk_size: Number of rows to read at a time
            
        Yields:
            Data records
        """
        filepath = Path(filepath)
        
        async with aiofiles.open(filepath, 'r', newline='', encoding='utf-8') as f:
            # Read header
            header_line = await f.readline()
            fieldnames = header_line.strip().split(',')
            
            # Read remaining lines in chunks
            lines = []
            async for line in f:
                lines.append(line.strip())
                
                if len(lines) >= chunk_size:
                    for row in lines:
                        values = row.split(',')
                        yield dict(zip(fieldnames, values))
                    lines = []
                    await asyncio.sleep(0)
            
            # Process remaining lines
            for row in lines:
                values = row.split(',')
                yield dict(zip(fieldnames, values))


# Global streaming exporter instance
streaming_exporter = StreamingExporter()


# Example usage
async def example_usage():
    """Example of how to use the streaming exporter."""
    
    # Example 1: Export from database to JSON
    async def example_1():
        stats = await streaming_exporter.export_from_database(
            db_query="SELECT * FROM tweets WHERE created_at > ?",
            db_params=('2024-01-01',),
            output_path='tweets_2024.json',
            format='json'
        )
        print(f"Export completed: {stats}")
    
    # Example 2: Export from list to CSV
    async def example_2():
        # Sample data
        tweets = [
            {'id': '1', 'text': 'Hello world', 'username': 'user1'},
            {'id': '2', 'text': 'Another tweet', 'username': 'user2'},
            # ... more tweets
        ]
        
        # Create async generator
        async def data_source():
            async for tweet in AsyncDataSource.from_list(tweets):
                yield tweet
        
        stats = await streaming_exporter.export_csv_stream(
            data_source(),
            'tweets.csv',
            fieldnames=['id', 'text', 'username']
        )
        print(f"CSV export completed: {stats}")
    
    # Example 3: Export to JSONL
    async def example_3():
        async def data_source():
            for i in range(1000):
                yield {'id': i, 'data': f'Record {i}'}
                await asyncio.sleep(0)  # Yield control
        
        stats = await streaming_exporter.export_jsonl_stream(
            data_source(),
            'data.jsonl'
        )
        print(f"JSONL export completed: {stats}")
    
    # Run examples
    await example_1()
    await example_2()
    await example_3()