-- Para descobrir os números das atualizações
select atualizacao
  from ceps.dne_delta_localidade
 group by atualizacao
 order by atualizacao;
 
-- Fazendo para a 1ª atualização => 25012
set @atualizacao=25012;
select * from ceps.dne_delta_localidade where atualizacao=@atualizacao;

-- Qual a diferença entre as informações a serem atualizadas
select d.loc_nu
     , a.ufe_sg loc_nu_ant, d.ufe_sg loc_nu_dep
     , a.loc_no loc_no_ant, d.loc_no loc_no_dep
     , a.cep cep_ant, d.cep cep_dep
     , a.loc_in_sit loc_in_sit_ant, d.loc_in_sit loc_in_sit_dep
     , a.loc_in_tipo_loc loc_in_tipo_loc_ant, d.loc_in_tipo_loc loc_in_tipo_loc_dep
     , a.loc_nu_sub loc_nu_sub_ant, d.loc_nu_sub loc_nu_sub_dep
     , a.loc_no_abrev loc_no_abrev_ant, d.loc_no_abrev loc_no_abrev_dep
     , a.mun_nu mun_nu_ant, d.mun_nu mun_nu_dep
  from ceps.dne_delta_localidade d left join ceps.dne_localidade a on d.loc_nu=a.loc_nu
 where d.atualizacao=@atualizacao
   and d.loc_operacao='UPD';
   
-- Fazendo a atualizacao
update ceps.dne_delta_localidade d, ceps.dne_localidade a
   set a.ufe_sg=d.ufe_sg
     , a.loc_no=d.loc_no
     , a.cep=d.cep
     , a.loc_in_sit=d.loc_in_sit
     , a.loc_in_tipo_loc=d.loc_in_tipo_loc
     , a.loc_nu_sub=d.loc_nu_sub
     , a.loc_no_abrev=d.loc_no_abrev
     , a.mun_nu=d.mun_nu
 where d.atualizacao=@atualizacao
   and d.loc_nu=a.loc_nu
   and d.loc_operacao='UPD';


