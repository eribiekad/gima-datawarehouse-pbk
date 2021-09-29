#!/usr/bin/env groovy
library identifier: 'gima-jenkins-shared-library@master', retriever: modernSCM(github(credentialsId: 'kad-jenkins-gima', repository: 'gima-jenkins-shared-library', repoOwner: 'kadaster-it'))

final String baseStackName = 'datawarehouse-pbk'

kubernetesDeploy(baseStackName)
