import csv
import random
from random import randint
from collections import deque

class Process:
    def __init__(self, PID, CPUCycles, MemorySize):
        self.PID = PID
        self.CPUCycles = CPUCycles
        self.MemorySize = MemorySize
        self.RemCPUCycles = CPUCycles

        # statistics for each process
        self.arrivalTime = 0
        self.completedTime = None


    def __str__(self):
        # returns a string for the process to be printed
        return "PID={}, CPUCycles={}/{}, Size={}, Completed={}".format(self.PID, self.RemCPUCycles, self.CPUCycles, self.MemorySize, self.completedTime)

class ProcessDoneEvent:
    def __init__(self, process, timestamp, cpu):
        self.process = process
        self.timestamp = timestamp
        self.cpu = cpu

class Simulation:
    TimeQuantum = 10 ** 10
    def __init__(self):
        # 6 CPUs
        self.CPUs = ["Pa", "Pb", "Pc", "Pd", "Pe", "Pf"]
        # need a round queue of CPUs
        self.idleCPUs = deque(self.CPUs, maxlen=len(self.CPUs))
        # incoming process queue, to be assigned to CPUs
        self.incomingProcesses = deque()

        self.contextSwitches = 0

        # list of completed processes
        self.completedProcess = []

        # a queue of events
        self.events = []
        # simulation time
        self.currentTime = 0

    def isDone(self):
        return len(self.incomingProcesses) == 0 and len(self.idleCPUs) == len(self.CPUs)

    def run(self):
        while not self.isDone():
            # check if there is any CPU idle and a process incoming
            if len(self.incomingProcesses) > 0 and len(self.idleCPUs) > 0:
                # get the CPU, first in the queue
                cpu = self.idleCPUs.popleft()
                # get the process
                process = self.incomingProcesses.popleft()
                print("Simo time {}: Process {} starts on CPU {}".format(self.currentTime, process, cpu))
                # place an even to remove the process from the queue after time quantum
                # even if it is not complete
                completedEvent = ProcessDoneEvent(process, self.currentTime + min(Simulation.TimeQuantum, process.RemCPUCycles), cpu)
                # put an event to the queue
                self.events.append(completedEvent)
                self.contextSwitches += 1
            else:
                # no more idle CPUs or processes
                # check the event when the next process is complete
                # find the event with the smallest time
                minTimeEventIndex = 0
                for i, ev in enumerate(self.events):
                    if ev.timestamp < self.events[minTimeEventIndex].timestamp:
                        minTimeEventIndex = i
                minTimeEvent = self.events[minTimeEventIndex]
                # remove the event from queue
                self.events.pop(minTimeEventIndex)
                # advance our simulation time to the event
                self.currentTime = minTimeEvent.timestamp
                process = minTimeEvent.process
                cpu = minTimeEvent.cpu
                # check the process remaining CPUCycles
                if process.RemCPUCycles <= Simulation.TimeQuantum:
                    # the process was completed in the last time quantum
                    # put the process to completed
                    process.RemCPUCycles = 0
                    # completed at this time of the simulation
                    process.completedTime = self.currentTime 
                    self.completedProcess.append(process)
                else:
                    # subtract the number of cycles from the quantum
                    process.RemCPUCycles -= Simulation.TimeQuantum
                    # place back to the incoming queue
                    self.incomingProcesses.append(process)
                print("Simo time {}: Process {} completes on CPU {}".format(self.currentTime, process, cpu))
                # put the CPU back to idle
                self.idleCPUs.append(cpu)




    def printStats(self):
        print("Statistics:")
        nProcesses = len(self.completedProcess)
        totalWaitTime = 0
        totalTurnAroundTime = 0
        for process in self.completedProcess:
            turnAroundTime = process.completedTime - process.arrivalTime
            totalTurnAroundTime += turnAroundTime
            waitTime = turnAroundTime - process.CPUCycles
            totalWaitTime += waitTime
        # print the averages
        print("Average wait time (seconds at 4GHz): {}".format(totalWaitTime / nProcesses / 4E9))
        print("Average turnaround time (seconds at 4GHz): {}".format(totalTurnAroundTime / nProcesses / 4E9))
        print("Context switches: {}".format(self.contextSwitches))






if __name__ == "__main__":
    # create a simulation instance
    simo = Simulation()
    with open('processes8_16_arrival.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csvit = iter(csv_reader)
        csvHeader = next(csvit)
        for PID, CPUCycles, MemorySize, Arrival in csvit:
            PID = int(PID)
            CPUCycles = int(CPUCycles)
            MemorySize = int(MemorySize)
            # create a process, place in the queue
            simo.incomingProcesses.append(Process(PID, CPUCycles, MemorySize))
    # processes are read, ready to run
    simo.run()
    simo.printStats()
          