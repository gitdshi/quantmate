"""Redis-based Job Storage and Management."""
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json
from redis import Redis
from rq.job import Job
from rq import Queue

from app.api.config import get_settings

settings = get_settings()


class JobStorage:
    """Redis-based job storage and management."""
    
    def __init__(self):
        """Initialize Redis connection."""
        self.redis = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            decode_responses=True  # Decode strings for metadata
        )
        self.prefix = "tradermate:job:"
        self.result_prefix = "tradermate:result:"
        self.ttl = 86400 * 7  # Keep results for 7 days
    
    def save_job_metadata(self, job_id: str, metadata: Dict[str, Any]) -> None:
        """
        Save job metadata to Redis.
        
        Args:
            job_id: Job ID
            metadata: Job metadata dict
        """
        key = f"{self.prefix}{job_id}"
        metadata["updated_at"] = datetime.now().isoformat()
        self.redis.setex(key, self.ttl, json.dumps(metadata))
    
    def get_job_metadata(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job metadata from Redis.
        
        Args:
            job_id: Job ID
        
        Returns:
            Job metadata dict or None
        """
        key = f"{self.prefix}{job_id}"
        data = self.redis.get(key)
        return json.loads(data) if data else None
    
    def save_result(self, job_id: str, result: Dict[str, Any]) -> None:
        """
        Save job result to Redis.
        
        Args:
            job_id: Job ID
            result: Result data
        """
        key = f"{self.result_prefix}{job_id}"
        self.redis.setex(key, self.ttl, json.dumps(result))
    
    def get_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job result from Redis.
        
        Args:
            job_id: Job ID
        
        Returns:
            Result dict or None
        """
        key = f"{self.result_prefix}{job_id}"
        data = self.redis.get(key)
        return json.loads(data) if data else None
    
    def update_job_status(self, job_id: str, status: str, **kwargs) -> None:
        """
        Update job status.
        
        Args:
            job_id: Job ID
            status: New status (queued, started, finished, failed, cancelled)
            **kwargs: Additional fields to update
        """
        metadata = self.get_job_metadata(job_id)
        if metadata:
            metadata["status"] = status
            metadata.update(kwargs)
            self.save_job_metadata(job_id, metadata)
    
    def update_progress(self, job_id: str, progress: float, message: str = "") -> None:
        """
        Update job progress.
        
        Args:
            job_id: Job ID
            progress: Progress percentage (0-100)
            message: Optional progress message
        """
        metadata = self.get_job_metadata(job_id)
        if metadata:
            metadata["progress"] = progress
            if message:
                metadata["progress_message"] = message
            self.save_job_metadata(job_id, metadata)
    
    def list_user_jobs(
        self, 
        user_id: int, 
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        List jobs for a user.
        
        Args:
            user_id: User ID
            status: Optional status filter
            limit: Maximum number of jobs to return
        
        Returns:
            List of job metadata dicts
        """
        pattern = f"{self.prefix}*"
        jobs = []
        
        for key in self.redis.scan_iter(match=pattern, count=100):
            data = self.redis.get(key)
            if data:
                metadata = json.loads(data)
                if metadata.get("user_id") == user_id:
                    if status is None or metadata.get("status") == status:
                        jobs.append(metadata)
        
        # Sort by creation time (newest first)
        jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return jobs[:limit]
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete job metadata and result.
        
        Args:
            job_id: Job ID
        
        Returns:
            True if deleted, False if not found
        """
        meta_key = f"{self.prefix}{job_id}"
        result_key = f"{self.result_prefix}{job_id}"
        
        deleted = self.redis.delete(meta_key, result_key)
        return deleted > 0
    
    def cancel_job(self, job_id: str, queue: Queue) -> bool:
        """
        Cancel a running job.
        
        Args:
            job_id: Job ID
            queue: RQ Queue instance
        
        Returns:
            True if cancelled, False if not possible
        """
        try:
            job = Job.fetch(job_id, connection=self.redis)
            
            # Can only cancel queued or started jobs
            if job.get_status() in ['queued', 'started']:
                job.cancel()
                self.update_job_status(job_id, 'cancelled')
                return True
            
            return False
            
        except Exception as e:
            print(f"Error cancelling job {job_id}: {e}")
            return False
    
    def cleanup_old_jobs(self, days: int = 7) -> int:
        """
        Clean up jobs older than specified days.
        
        Args:
            days: Number of days to keep
        
        Returns:
            Number of jobs deleted
        """
        cutoff = datetime.now() - timedelta(days=days)
        pattern = f"{self.prefix}*"
        deleted = 0
        
        for key in self.redis.scan_iter(match=pattern, count=100):
            data = self.redis.get(key)
            if data:
                metadata = json.loads(data)
                created_at = datetime.fromisoformat(metadata.get("created_at", ""))
                
                if created_at < cutoff:
                    job_id = metadata.get("job_id")
                    if self.delete_job(job_id):
                        deleted += 1
        
        return deleted
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about job queues.
        
        Returns:
            Dict with queue statistics
        """
        from app.api.worker.config import QUEUES
        
        stats = {}
        for name, queue in QUEUES.items():
            stats[name] = {
                "queued": len(queue),
                "failed": queue.failed_job_registry.count,
                "finished": queue.finished_job_registry.count,
                "started": queue.started_job_registry.count,
            }
        
        return stats


# Singleton instance
_job_storage = None


def get_job_storage() -> JobStorage:
    """Get JobStorage singleton instance."""
    global _job_storage
    if _job_storage is None:
        _job_storage = JobStorage()
    return _job_storage
