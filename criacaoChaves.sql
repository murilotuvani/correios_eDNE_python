ALTER TABLE `cep`.`dne_unidade_operacional` 
CHANGE COLUMN `uop_nu` `uop_nu` INT UNSIGNED NOT NULL ,
ADD PRIMARY KEY (`uop_nu`);

ALTER TABLE `cep`.`dne_localidade` 
CHANGE COLUMN `loc_nu` `loc_nu` INT UNSIGNED NOT NULL ,
ADD PRIMARY KEY (`loc_nu`),
ADD INDEX `ix_dne_loca_nome` (`loc_no` ASC) VISIBLE,
ADD INDEX `ix_dne_loca_uf` (`ufe_sg` ASC) VISIBLE,
DROP INDEX `ix_dne_localidade_loc_nu` ;

ALTER TABLE `cep`.`dne_logradouro` 
CHANGE COLUMN `log_nu` `log_nu` INT UNSIGNED NOT NULL ,
ADD PRIMARY KEY (`log_nu`),
ADD INDEX `ix_dne_logr_nome` (`log_no` ASC) INVISIBLE,
ADD INDEX `ix_dne_logr_cep` (`cep` ASC) VISIBLE,
ADD INDEX `fk_dne_logr_loca_idx` (`loc_nu` ASC) VISIBLE,
DROP INDEX `ix_dne_logradouro_log_nu` ;

ALTER TABLE `cep`.`dne_logradouro` 
ADD CONSTRAINT `fk_dne_logr_loca`
  FOREIGN KEY (`loc_nu`)
  REFERENCES `cep`.`dne_localidade` (`loc_nu`)
  ON DELETE RESTRICT
  ON UPDATE RESTRICT;

ALTER TABLE `cep`.`dne_grande_usuario` 
CHANGE COLUMN `gru_nu` `gru_nu` INT UNSIGNED NOT NULL ,
ADD PRIMARY KEY (`gru_nu`),
ADD INDEX `fk_gran_usua_loca_idx` (`loc_nu` ASC) VISIBLE,
ADD UNIQUE INDEX `uk_gran_usua_cep` (`cep` ASC) VISIBLE,
DROP INDEX `ix_dne_grande_usuario_gru_nu` ;

ALTER TABLE `cep`.`dne_grande_usuario` 
ADD CONSTRAINT `fk_gran_usua_loca`
  FOREIGN KEY (`loc_nu`)
  REFERENCES `cep`.`dne_localidade` (`loc_nu`)
  ON DELETE RESTRICT
  ON UPDATE RESTRICT;

ALTER TABLE `cep`.`dne_unidade_operacional` 
ADD UNIQUE INDEX `uk_uid_oper_cep` (`cep` ASC) VISIBLE,
DROP INDEX `ix_dne_unidade_operacional_uop_nu` ;

ALTER TABLE `cep`.`dne_caixa_postal_comunitaria` 
CHANGE COLUMN `cpc_nu` `cpc_nu` INT UNSIGNED NOT NULL ,
ADD PRIMARY KEY (`cpc_nu`),
ADD UNIQUE INDEX `uk_caix_post_comu_cep` (`cep` ASC) INVISIBLE,
ADD INDEX `fk_caix_post_comu_loca_idx` (`loc_nu` ASC) VISIBLE,
DROP INDEX `ix_dne_caixa_postal_comunitaria_cpc_nu` ;

ALTER TABLE `cep`.`dne_caixa_postal_comunitaria` 
ADD CONSTRAINT `fk_caix_post_comu_loca`
  FOREIGN KEY (`loc_nu`)
  REFERENCES `cep`.`dne_localidade` (`loc_nu`)
  ON DELETE RESTRICT
  ON UPDATE RESTRICT;

ALTER TABLE `cep`.`dne_bairro` 
CHANGE COLUMN `bai_nu` `bai_nu` INT UNSIGNED NOT NULL ,
ADD PRIMARY KEY (`bai_nu`),
ADD INDEX `ix_bair_nome` (`bai_no` ASC) INVISIBLE,
ADD INDEX `fk_bair_loca_idx` (`loc_nu` ASC) VISIBLE,
DROP INDEX `ix_dne_bairro_bai_nu` ;

ALTER TABLE `cep`.`dne_bairro` 
ADD CONSTRAINT `fk_bair_loca`
  FOREIGN KEY (`loc_nu`)
  REFERENCES `cep`.`dne_localidade` (`loc_nu`)
  ON DELETE RESTRICT
  ON UPDATE RESTRICT;

