//
//  main.c
//  Pro1
//
//  Created by Cheng on 2017/10/8.
//  Copyright © 2017年 Cheng. All rights reserved.
//

#define LEN_of_LINE 1024
#define NUM_of_CMD 128
#define NUM_of_PIPE 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>


char *readCmd(void);
char **parseCmd(char *line);
int execute(char **cmd);
int ifInputRedir(char *line);
int ifOutputRedir(char *line);
void quotes(char *line, int flag);
int numChar(char *str, char cha);
int ve482cd(char **cmd);
int ve482exit(char **cmd);
int ve482pwd(char **cmd);
void split(char *line);
void execute_pipe(int i, int n);
void simple_execute(char *cmd[]);
void deleteCmd(int index, char**cmd);
void findRed(char**cmd);
void outRed(char *file, int index, char**cmd);
void inRed(char *file, int index, char**cmd);
void outRed2(char *file, int index, char**cmd);


char ** cmds[NUM_of_PIPE];
int fd_out;

int main() {
    char **cmd;
    int status=1;
    while(status!=0)
    {
        printf("ve482sh $ ");
        char *line=readCmd();
        quotes(line, 0);
        int n=numChar(line, '|')+1;
        if (n==1)
        {
            cmd=parseCmd(line);
            findRed(cmd);
            status=execute(cmd);
            free(cmd);
        }
        else
        {
            pid_t pid=fork();
            if (pid==0)
            {
                split(line);
                execute_pipe(0, n);
                exit(0);
            }
            waitpid(pid,NULL,0);
        }
        free(line);
    }
    return 0;
}


char *readCmd(void)
{//read the current line in 482shell
    char *line=NULL;
    ssize_t n=0;
    getline(&line, &n, stdin);
    
    return line;
}

/*=================================================*
 *                                                 *
 *         Simple parse without ' " < > |          *
 *                                                 *
 *=================================================*/
char **parseCmd(char *line)
{//interept the line
    char *simpleParse=" \n";
    size_t cmdsize=NUM_of_CMD;
    char **cmd=(char**)malloc(cmdsize*sizeof(char*));
    char *parsedCmd;
    int i=0;
    
    parsedCmd=strtok(line, simpleParse);
    while(parsedCmd!=NULL)
    {
        cmd[i]=parsedCmd;
        i++;
        parsedCmd=strtok(NULL, simpleParse);
    }
    
    return cmd;
}

int execute(char **cmd)
{//excute the command
    pid_t forkP;
    pid_t waitP;
    int status;
    
    if (cmd[0]==NULL)
        return 1;
    
    else if (strcmp(cmd[0], "cd")==0)
        return ve482cd(cmd);
    
    else if (strcmp(cmd[0], "exit")==0)
        return ve482exit(cmd);
    
    else if (strcmp(cmd[0],"pwd")==0)
        return ve482pwd(cmd);
    else
    {
        forkP=fork();
        
        if(forkP==0)
        {
            execvp(cmd[0], cmd);
        }
        else if(forkP>0)
        {
            do {
                waitP = waitpid(forkP, &status, WUNTRACED);
            } while (!WIFEXITED(status) && !WIFSIGNALED(status));//waitpid
        }
        else if(forkP<0)
        {
            perror("Wrong execution!");
        }
        return 1;
    }
}



int numChar(char *str, char cha)
{
    int s=0;
    int i=0;
    while(str[i]!='\0')
    {
        if (str[i]==cha)
            s++;
        i++;
    }
    return s;
}


int ve482cd(char **cmd)
{
    if (cmd[1] == NULL)
        perror("Missing directory name");
    else
    {
        if (chdir(cmd[1]) != 0)
            perror("No such directory!");
    }
    return 1;
}


int ve482exit(char **cmd)
{
    return 0;
}

int ve482pwd(char **cmd)
{
    char address[LEN_of_LINE];
    getcwd(address,sizeof(address));
    printf("%s \n", address);
    return 1;
}

void split(char *line)
{
    char *temp;
    int i=0;
    char *sLine = strtok_r(line, "|", &temp);
    while (sLine!=NULL) {
        cmds[i]=parseCmd(sLine);
        findRed(cmds[i]);
        i++;
        sLine = strtok_r(NULL, "|", &temp);
    }
}

void execute_pipe(int i, int n)
{
    if (i == n-1)
        simple_execute(cmds[i]);
    
    int pipefd[2];
    pipe(pipefd);
    if (fork() == 0)
    {
        dup2(pipefd[1], 1);
        close(pipefd[0]);
        close(pipefd[1]);
        simple_execute(cmds[i]);
    }
    dup2(pipefd[0], 0);
    close(pipefd[0]);
    close(pipefd[1]);
    execute_pipe(i+1, n);
}

void simple_execute(char *cmd[])
{
    int result;
    result=execvp(cmd[0], cmd);
    if (result==-1)
        printf("Wrong pipe!\n");
    exit(1);
}


void deleteCmd(int index, char**cmd)
{
    int i=index;
    while(cmd[i+1]!=NULL)
    {
        cmd[i]=cmd[i+1];
        i++;
    }
    cmd[i]=NULL;
    
}

void inRed(char *file, int index, char**cmd)
{
    int fd;
    fd=open(file ,O_RDONLY);
    deleteCmd(index, cmd);
    deleteCmd(index-1, cmd);
    if (fd==-1)
    {
        printf("Cannot open file: %s\n", file);
        return;
    }
    dup2(fd,0);
    close(fd);
}



void outRed(char *file, int index, char**cmd)
{
    int fd;
    fd=open(file,O_CREAT|O_RDWR,0666);
    deleteCmd(index, cmd);
    deleteCmd(index-1, cmd);
    
    if (fd==-1)
    {
        printf("Cannot open file: %s\n", file);
        return;
    }
    fd_out=fd;
    dup2(fd,1);
    close(fd);
}

void outRed2(char *file, int index, char**cmd)
{
    int fd;
    fd=open(file,O_CREAT|O_RDWR|O_APPEND,0666);
    deleteCmd(index, cmd);
    deleteCmd(index-1, cmd);
    if (fd==-1)
    {
        printf("Cannot open file: %s\n", file);
        return;
    }
    dup2(fd,1);
    close(fd);
}

void findRed(char**cmd)
{
    int i=0;
    while(cmd[i]!=NULL)
    {
        char* currCmd=cmd[i];
        if (strcmp(currCmd, "<")==0)
            inRed(cmd[i+1], i+1, cmd);
        else if (strcmp(currCmd, ">")==0)
            outRed(cmd[i+1], i+1, cmd);
        else if (strcmp(currCmd, ">>")==0)
            outRed2(cmd[i+1], i+1, cmd);
        else i++;
    }
    
}

void quotes(char *line, int flag)
{
    int i=0;
    int l=strlen(line);
    //if flag=1: there has been one single quote, waiting for another one
    //if flag=2: there has been one double quote, waiting for another one
    while(line[i]!='\0')
    {
        if (flag==0)
        {
            if (line[i]=='\'')
            {
                flag=1;
                memmove(line+i, line+i+1, l-i-1);
                l--;
                line[l-1]='\0';
                continue;
            }
            if (line[i]=='\"')
            {
                flag=2;
                memmove(line+i, line+i+1, l-i-1);
                l--;
                line[l-1]='\0';
                continue;
            }
        }
        if (flag==1 && line[i]=='\'')
        {
            flag=0;
            memmove(line+i, line+i+1, l-i-1);
            l--;
            line[l-1]='\0';
            continue;
        }
        
        if (flag==2 && line[i]=='\"')
        {
            flag=0;
            memmove(line+i, line+i+1, l-i-1);
            l--;
            line[l-1]='\0';
            continue;
        }
        i++;
    }
    if (flag!=0)
    {
        printf("> ");
        char* newline=readCmd();
        strcat(line, newline);
        free(newline);
        quotes(line, flag);
    }
}





