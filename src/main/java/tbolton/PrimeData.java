package tbolton;

import java.util.UUID;

public class PrimeData {
        private UUID uuid;
        private Long timestampCreatedAt;
        private Long timestampProcessStart;
        private Long timestampProcessEnd;
        private Long number;
        private boolean isPrime;

        public PrimeData() { super(); }

        public PrimeData(UUID uuid, Long createdAt, Long number)
        {
            this.uuid = uuid;
            this.timestampCreatedAt = createdAt;
            this.timestampProcessStart = 0L;
            this.timestampProcessEnd = 0L;
            this.number = number;
            this.isPrime = false;
        }
        public PrimeData(PrimeData other)
        {
            this.uuid = other.uuid;
            this.timestampCreatedAt = other.timestampCreatedAt;
            this.timestampProcessStart = other.timestampProcessStart;
            this.timestampProcessEnd = other.timestampProcessEnd;
            this.number = other.number;
            this.isPrime = other.isPrime;
        }
        public UUID getUuid() { return uuid; }
        public void setUuid(UUID uuid) { this.uuid = uuid; }
        public Long getTimestampCreatedAt() { return timestampCreatedAt; }
        public void setTimestampCreatedAt(Long timestampCreatedAt) { this.timestampCreatedAt = timestampCreatedAt; }
        public Long getTimestampProcessStart() { return timestampProcessStart; }
        public void setTimestampProcessStart(Long timestampProcessStart) { this.timestampProcessStart = timestampProcessStart; }
        public Long getTimestampProcessEnd() { return timestampProcessEnd; }
        public void setTimestampProcessEnd(Long timestampProcessEnd) { this.timestampProcessEnd = timestampProcessEnd; }
        public Long getNumber() { return number; }
        public void setNumber(Long number) { this.number = number; }
        public boolean getIsPrime() { return isPrime; }
        public void setIsPrime(boolean isPrime) { this.isPrime = isPrime; }
    }
