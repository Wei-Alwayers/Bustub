<img src="logo/bustub-whiteborder.svg" alt="BusTub Logo" height="200">

# **Bustub: A Relational Database Management System**

## **Overview**
Bustub is an educational in-memory database management system (DBMS) built as part of the CMU 15-445 course. The project involves designing and implementing key components of a DBMS, such as storage, buffer pool management, and query execution, with a focus on hands-on learning of database internals.

This repository showcases my implementation of Bustub, demonstrating my skills in systems programming, algorithm design, and database architecture.

## **Key Features**
- **Buffer Pool Manager**: Implemented an efficient buffer pool manager to handle page replacement using LRU (Least Recently Used) caching policies.
- **Indexing**: Designed and implemented B+ Tree indexing to support efficient range queries and data retrieval.
- **Query Execution**: Developed a query execution engine capable of handling SQL-like queries with various operators, including joins and aggregations.
- **Concurrency Control**: Integrated concurrency mechanisms to ensure data consistency under multi-threaded environments.
- **Transaction Management**: Supported atomicity, consistency, isolation, and durability (ACID) properties for transactions.

## **Skills Demonstrated**
- **Programming Languages**: C++ (Modern C++ features like smart pointers, RAII).
- **Database Concepts**: Buffer management, indexing, and query optimization.
- **Systems Programming**: Multi-threading, memory management, and performance optimization.
- **Problem Solving**: Debugging complex systems and ensuring functionality through unit testing.
## Cloning this Repository

The following instructions are adapted from the Github documentation on [duplicating a repository](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/creating-a-repository-on-github/duplicating-a-repository). The procedure below walks you through creating a private BusTub repository that you can use for development.

1. Go [here](https://github.com/new) to create a new repository under your account. Pick a name (e.g. `bustub-private`) and select **Private** for the repository visibility level.
2. On your development machine, create a bare clone of the public BusTub repository:
   ```
   $ git clone --bare https://github.com/cmu-db/bustub.git bustub-public
   ```
3. Next, [mirror](https://git-scm.com/docs/git-push#Documentation/git-push.txt---mirror) the public BusTub repository to your own private BusTub repository. Suppose your GitHub name is `student` and your repository name is `bustub-private`. The procedure for mirroring the repository is then:
   ```
   $ cd bustub-public
   
   # If you pull / push over HTTPS
   $ git push https://github.com/student/bustub-private.git master

   # If you pull / push over SSH
   $ git push git@github.com:student/bustub-private.git master
   ```
   This copies everything in the public BusTub repository to your own private repository. You can now delete your local clone of the public repository:
   ```
   $ cd ..
   $ rm -rf bustub-public
   ```
4. Clone your private repository to your development machine:
   ```
   # If you pull / push over HTTPS
   $ git clone https://github.com/student/bustub-private.git

   # If you pull / push over SSH
   $ git clone git@github.com:student/bustub-private.git
   ```
5. Add the public BusTub repository as a second remote. This allows you to retrieve changes from the CMU-DB repository and merge them with your solution throughout the semester:
   ```
   $ git remote add public https://github.com/cmu-db/bustub.git
   ```
   You can verify that the remote was added with the following command:
   ```
   $ git remote -v
   origin	https://github.com/student/bustub-private.git (fetch)
   origin	https://github.com/student/bustub-private.git (push)
   public	https://github.com/cmu-db/bustub.git (fetch)
   public	https://github.com/cmu-db/bustub.git (push)
   ```
6. You can now pull in changes from the public BusTub repository as needed with:
   ```
   $ git pull public master
   ```
7. **Disable GitHub Actions** from the project settings of your private repository, otherwise you may run out of GitHub Actions quota.
   ```
   Settings > Actions > General > Actions permissions > Disable actions.
   ```

We suggest working on your projects in separate branches. If you do not understand how Git branches work, [learn how](https://git-scm.com/book/en/v2/Git-Branching-Basic-Branching-and-Merging). If you fail to do this, you might lose all your work at some point in the semester, and nobody will be able to help you.

## Build

We recommend developing BusTub on Ubuntu 20.04, Ubuntu 22.04, or macOS (M1/M2/Intel). We do not support any other environments (i.e., do not open issues or come to office hours to debug them). We do not support WSL.

### Linux / Mac (Recommended)

To ensure that you have the proper packages on your machine, run the following script to automatically install them:

```
# Linux
$ sudo build_support/packages.sh
# macOS
$ build_support/packages.sh
```

Then run the following commands to build the system:

```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

If you want to compile the system in debug mode, pass in the following flag to cmake:
Debug mode:

```
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make -j`nproc`
```
This enables [AddressSanitizer](https://github.com/google/sanitizers) by default.

If you want to use other sanitizers,


```
$ cmake -DCMAKE_BUILD_TYPE=Debug -DBUSTUB_SANITIZER=thread ..
$ make -j`nproc`
```

### Windows (Not Guaranteed to Work)

If you are using Windows 10, you can use the Windows Subsystem for Linux (WSL) to develop, build, and test Bustub. All you need is to [Install WSL](https://docs.microsoft.com/en-us/windows/wsl/install-win10). You can just choose "Ubuntu" (no specific version) in Microsoft Store. Then, enter WSL and follow the above instructions.

If you are using CLion, it also [works with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends).

### Vagrant (Not Guaranteed to Work)

First, make sure you have Vagrant and Virtualbox installed
```
$ sudo apt update
$ sudo apt install vagrant virtualbox
```

From the repository directory, run this command to create and start a Vagrant box:

```
$ vagrant up
```

This will start a Vagrant box running Ubuntu 20.02 in the background with all the packages needed. To access it, type

```
$ vagrant ssh
```

to open a shell within the box. You can find Bustub's code mounted at `/bustub` and run the commands mentioned above like normal.

### Docker (Not Guaranteed to Work)

First, make sure that you have docker installed:
```
$ sudo apt update
$ sudo apt install docker
```

From the repository directory, run these commands to create a Docker image and container:

```
$ docker build . -t bustub
$ docker create -t -i --name bustub -v $(pwd):/bustub bustub bash
```

This will create a Docker image and container. To run it, type:

```
$ docker start -a -i bustub
```

to open a shell within the box. You can find Bustub's code mounted at `/bustub` and run the commands mentioned above like normal.

## Testing

```
$ cd build
$ make check-tests
```

## **Accomplishments**
- **High Performance**: Achieved high efficiency in query execution and page replacement tests, surpassing baseline benchmarks.
- **Robust Testing**: Implemented and passed extensive unit tests to validate the correctness of each module.
- **Real-World Applications**: Gained foundational knowledge of DBMS internals, relevant to modern database technologies like PostgreSQL and MySQL.

## **What I Learned**
- Hands-on understanding of database internals and their interaction with the operating system.
- Practical experience with designing modular and maintainable software architectures.
- Techniques for optimizing algorithms and data structures for real-world systems.

## **Contact**
Feel free to reach out to discuss my implementation or provide feedback:
- **Email**: haoming.wei321@gmail.com
- **LinkedIn**: [Leo Wei](https://www.linkedin.com/in/haoming-wei/)
